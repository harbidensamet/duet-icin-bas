require('dotenv').config();
const express = require('express');
const cors = require('cors');
const admin = require('firebase-admin');
const { google } = require('googleapis');
const WebSocket = require('ws');

// ── Firebase init ──────────────────────────────────────────────────────────────
const serviceAccount = require(process.env.FIREBASE_SERVICE_ACCOUNT_PATH || './firebase-service-account.json');
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL,
});
const db = admin.database();

// ── Config ─────────────────────────────────────────────────────────────────────
const DEFAULT_EMOJI = process.env.TRIGGER_EMOJI || '🤍';
let TRIGGER_EMOJI = DEFAULT_EMOJI; // round başlayınca güncellenir
const PORT = parseInt(process.env.PORT || '3001', 10);

// ── State ──────────────────────────────────────────────────────────────────────
let currentRoundId = null;   // Firebase key for active round
let roundActive = false;

// ── Firebase helpers ───────────────────────────────────────────────────────────
async function recordHit(platform, userId, displayName) {
  if (!roundActive || !currentRoundId) return;

  const ref = db.ref(`rounds/${currentRoundId}`);

  // Atomic update: increment total, set unique participant if new
  await ref.transaction((round) => {
    if (!round) return round;

    const participants = round.participants || {};
    const isNew = !participants[userId];
    participants[userId] = {
      platform,
      displayName: displayName || userId,
      firstSeen: participants[userId]?.firstSeen || Date.now(),
    };

    return {
      ...round,
      participants,
      totalCount: (round.totalCount || 0) + 1,
      uniqueCount: Object.keys(participants).length,
    };
  });
}

// ── YouTube Live Chat ──────────────────────────────────────────────────────────
const youtube = google.youtube({ version: 'v3', auth: process.env.YOUTUBE_API_KEY });
let ytPollTimer = null;
let ytChatId = null;
let ytNextPageToken = null;

async function getActiveLiveChatId() {
  // 1. Explicit video ID from env
  if (process.env.YOUTUBE_VIDEO_ID) {
    const res = await youtube.videos.list({
      part: 'liveStreamingDetails',
      id: process.env.YOUTUBE_VIDEO_ID,
    });
    return res.data.items?.[0]?.liveStreamingDetails?.activeLiveChatId || null;
  }

  // 2. Find active broadcast from channel
  if (process.env.YOUTUBE_CHANNEL_ID) {
    const res = await youtube.search.list({
      part: 'id',
      channelId: process.env.YOUTUBE_CHANNEL_ID,
      eventType: 'live',
      type: 'video',
      maxResults: 1,
    });
    const videoId = res.data.items?.[0]?.id?.videoId;
    if (!videoId) return null;

    const vRes = await youtube.videos.list({
      part: 'liveStreamingDetails',
      id: videoId,
    });
    return vRes.data.items?.[0]?.liveStreamingDetails?.activeLiveChatId || null;
  }

  return null;
}

async function pollYouTubeChat() {
  if (!roundActive) return;

  try {
    if (!ytChatId) {
      ytChatId = await getActiveLiveChatId();
      if (!ytChatId) {
        console.log('[YT] Aktif yayın bulunamadı, 30s sonra tekrar deneniyor...');
        ytPollTimer = setTimeout(pollYouTubeChat, 30_000);
        return;
      }
      console.log('[YT] Chat ID bulundu:', ytChatId);
    }

    const res = await youtube.liveChatMessages.list({
      liveChatId: ytChatId,
      part: 'snippet,authorDetails',
      pageToken: ytNextPageToken || undefined,
      maxResults: 2000,
    });

    ytNextPageToken = res.data.nextPageToken;
    const pollingMs = res.data.pollingIntervalMillis || 5000;

    for (const item of res.data.items || []) {
      const text = item.snippet?.displayMessage || '';
      if (text.includes(TRIGGER_EMOJI)) {
        const userId = item.authorDetails.channelId;
        const displayName = item.authorDetails.displayName;
        await recordHit('youtube', userId, displayName);
        console.log(`[YT] Hit: ${displayName} (${userId})`);
      }
    }

    ytPollTimer = setTimeout(pollYouTubeChat, pollingMs);
  } catch (err) {
    // Yayın bitmişse ya da quota aşıldıysa graceful handle
    if (err.code === 403 || err.code === 404) {
      console.warn('[YT] Chat erişimi kesildi:', err.message);
      ytChatId = null;
      ytPollTimer = setTimeout(pollYouTubeChat, 60_000);
    } else {
      console.error('[YT] Poll hatası:', err.message);
      ytPollTimer = setTimeout(pollYouTubeChat, 10_000);
    }
  }
}

function stopYouTubeChat() {
  clearTimeout(ytPollTimer);
  ytPollTimer = null;
  ytNextPageToken = null;
  // ytChatId'yi koruyalım — round tekrar başlarsa aynı yayın devam ediyor olabilir
}

// ── TikTok Live Chat ───────────────────────────────────────────────────────────
let ttConnection = null;

async function startTikTokChat() {
  if (!process.env.TIKTOK_USERNAME || !process.env.EULERSTREAM_API_KEY) {
    console.log('[TT] TIKTOK_USERNAME veya EULERSTREAM_API_KEY eksik, atlanıyor.');
    return;
  }

  const wsUrl = `wss://ws.eulerstream.com?uniqueId=${encodeURIComponent(process.env.TIKTOK_USERNAME)}&apiKey=${encodeURIComponent(process.env.EULERSTREAM_API_KEY)}`;

  function connect() {
    if (ttConnection) return;

    console.log('[TT] Bağlanılıyor...');
    const ws = new WebSocket(wsUrl);
    ttConnection = ws;

    ws.on('open', () => {
      console.log('[TT] Bağlandı:', process.env.TIKTOK_USERNAME);
    });

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        for (const evt of (msg.messages || [])) {
          if (evt.type !== 'WebcastChatMessage') continue;
          if (!roundActive) continue;
          const d = evt.data || {};
          const comment = d.comment || '';
          if (!comment.includes(TRIGGER_EMOJI)) continue;
          const user = d.user || {};
          const userId = String(user.userId || user.openId || Math.random());
          const displayName = user.nickname || userId;
          await recordHit('tiktok', `tt_${userId}`, displayName);
          console.log(`[TT] Hit: ${displayName} — "${comment}"`);
        }
      } catch (_) {}
    });

    ws.on('close', () => {
      console.log('[TT] Bağlantı kesildi, 10s sonra yeniden deneniyor...');
      ttConnection = null;
      if (roundActive) setTimeout(connect, 10_000);
    });

    ws.on('error', (err) => {
      console.warn('[TT] WebSocket hatası:', err.message);
      ttConnection = null;
    });
  }

  connect();
}

function stopTikTokChat() {
  if (ttConnection) {
    try { ttConnection.close(); } catch (_) {}
    ttConnection = null;
  }
}

// ── Round management ───────────────────────────────────────────────────────────
async function startRound(participantName, emoji, episode, location) {
  if (roundActive) {
    console.log('[Round] Zaten aktif round var, önce bitirin.');
    return { error: 'Round zaten aktif' };
  }

  const triggerEmoji = emoji || DEFAULT_EMOJI;
  TRIGGER_EMOJI = triggerEmoji; // chat dinleyicisini güncelle
  const roundRef = db.ref('rounds').push();
  currentRoundId = roundRef.key;
  roundActive = true;

  const roundData = {
    participantName: participantName || 'Katılımcı',
    emoji: triggerEmoji,
    episode: episode || '',
    location: location || '',
    startTime: Date.now(),
    active: true,
    totalCount: 0,
    uniqueCount: 0,
    participants: {},
  };

  await roundRef.set(roundData);
  await db.ref('currentRound').set(currentRoundId);

  console.log(`\n[Round] BAŞLADI → ${participantName} | Emoji: ${triggerEmoji} | ID: ${currentRoundId}`);

  // Chat okumayı başlat
  ytNextPageToken = null; // Yeni round = yeni mesajlardan başla
  pollYouTubeChat();
  await startTikTokChat();

  return { roundId: currentRoundId, participantName, emoji: triggerEmoji };
}

async function stopRound() {
  if (!roundActive || !currentRoundId) {
    return { error: 'Aktif round yok' };
  }

  stopYouTubeChat();
  stopTikTokChat();

  const roundRef = db.ref(`rounds/${currentRoundId}`);
  await roundRef.update({ active: false, endTime: Date.now() });
  await db.ref('currentRound').remove();

  const snapshot = await roundRef.once('value');
  const result = snapshot.val();

  console.log(`[Round] BİTTİ → Unique: ${result.uniqueCount} | Toplam: ${result.totalCount}`);

  roundActive = false;
  const finishedId = currentRoundId;
  currentRoundId = null;

  return {
    roundId: finishedId,
    participantName: result.participantName,
    emoji: result.emoji,
    uniqueCount: result.uniqueCount,
    totalCount: result.totalCount,
  };
}

// ── Express API ────────────────────────────────────────────────────────────────
const path = require('path');
const app = express();
app.use(cors());
app.use(express.json());

// round.html'i doğrudan serve et (http://localhost:3001/ ve /round.html)
app.use(express.static(path.join(__dirname, '..')));

app.get('/status', (req, res) => {
  res.json({
    roundActive,
    currentRoundId,
    triggerEmoji: TRIGGER_EMOJI,
    youtube: !!ytChatId,
    tiktok: !!ttConnection,
  });
});

app.post('/round/start', async (req, res) => {
  const { participantName, emoji, episode, location } = req.body;
  const result = await startRound(participantName, emoji, episode, location);
  res.json(result);
});

app.post('/round/stop', async (req, res) => {
  const result = await stopRound();
  res.json(result);
});

app.get('/rounds', async (req, res) => {
  const snapshot = await db.ref('rounds').orderByChild('startTime').limitToLast(50).once('value');
  const rounds = [];
  snapshot.forEach((child) => {
    const val = child.val();
    // participants objesini sayıya indir (privacy + boyut)
    const { participants, ...rest } = val;
    rounds.push({ id: child.key, ...rest });
  });
  res.json(rounds.reverse());
});

app.listen(PORT, () => {
  console.log(`\n🎵 Düet İçin Bas — Round Backend`);
  console.log(`   Port    : http://localhost:${PORT}`);
  console.log(`   Emoji   : ${TRIGGER_EMOJI}`);
  console.log(`   YouTube : ${process.env.YOUTUBE_API_KEY ? '✓' : '✗ API key yok'}`);
  console.log(`   TikTok  : ${process.env.TIKTOK_USERNAME ? process.env.TIKTOK_USERNAME : '✗ username yok'}\n`);
});
