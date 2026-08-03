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
let roundStartTime = 0;      // eski chat mesajlarını filtrelemek için

// ── In-memory sayım + toplu Firebase yazımı ────────────────────────────────────
// Her emoji için ayrı transaction yerine bellekte sayıp saniyede 1 kez yazıyoruz.
// 10-20k izleyicide saniyede yüzlerce emoji gelse bile Firebase'e tek istek gider.
let pending = null;      // { participants: Map<userId, {platform, displayName, firstSeen}>, total, dirty }
let flushTimer = null;

function sanitizeKey(key) {
  // Firebase key'lerinde . # $ / [ ] olamaz
  return String(key).replace(/[.#$/\[\]]/g, '_');
}

function recordHit(platform, userId, displayName) {
  if (!roundActive || !pending) return;
  const key = sanitizeKey(userId);
  pending.total += 1;
  if (!pending.participants.has(key)) {
    pending.participants.set(key, {
      platform,
      displayName: displayName || key,
      firstSeen: Date.now(),
    });
  }
  pending.dirty = true;
}

async function flushRound() {
  if (!pending || !pending.dirty || !currentRoundId) return;
  pending.dirty = false;
  const participantsObj = {};
  for (const [id, p] of pending.participants) participantsObj[id] = p;
  try {
    await db.ref(`rounds/${currentRoundId}`).update({
      totalCount: pending.total,
      uniqueCount: pending.participants.size,
      participants: participantsObj,
    });
  } catch (err) {
    pending.dirty = true; // yazamadıysak sonraki flush'ta tekrar dene
    console.error('[Firebase] Flush hatası:', err.message);
  }
}

// ── YouTube Live Chat ──────────────────────────────────────────────────────────
const youtube = google.youtube({ version: 'v3', auth: process.env.YOUTUBE_API_KEY });
let ytPollTimer = null;
let ytChatId = null;
let ytNextPageToken = null;
let ytVideoIdCache = null;    // bulunan video ID — pahalı search.list'i tekrarlamamak için
let ytVideoIdOverride = null; // host panelinden girilen video ID (search.list'e hiç gerek kalmaz)

function parseYouTubeVideoId(input) {
  if (!input) return null;
  const s = String(input).trim();
  // Ham 11 karakterlik ID
  if (/^[\w-]{11}$/.test(s)) return s;
  // watch?v=, youtu.be/, /live/ formatları
  const m = s.match(/(?:v=|youtu\.be\/|\/live\/|\/shorts\/)([\w-]{11})/);
  return m ? m[1] : null;
}

async function chatIdFromVideo(videoId) {
  const res = await youtube.videos.list({
    part: 'liveStreamingDetails',
    id: videoId,
  });
  return res.data.items?.[0]?.liveStreamingDetails?.activeLiveChatId || null;
}

async function getActiveLiveChatId() {
  // 1. Host panelinden girilen video (videos.list = 1 kota birimi)
  if (ytVideoIdOverride) {
    return chatIdFromVideo(ytVideoIdOverride);
  }

  // 2. .env'deki video ID
  if (process.env.YOUTUBE_VIDEO_ID) {
    return chatIdFromVideo(process.env.YOUTUBE_VIDEO_ID);
  }

  // 3. Daha önce bulunmuş video hâlâ canlı mı? (1 birim — 100 birimlik search'ten kaçın)
  if (ytVideoIdCache) {
    const chatId = await chatIdFromVideo(ytVideoIdCache);
    if (chatId) return chatId;
    ytVideoIdCache = null; // yayın bitmiş, cache'i temizle
  }

  // 4. Kanaldan aktif yayın ara (search.list = 100 kota birimi — son çare)
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
    ytVideoIdCache = videoId;
    return chatIdFromVideo(videoId);
  }

  return null;
}

async function pollYouTubeChat() {
  if (!roundActive) return;

  try {
    if (!ytChatId) {
      ytChatId = await getActiveLiveChatId();
      if (!ytChatId) {
        // 60s bekle — search.list 100 birim yakıyor, sık deneme günlük kotayı bitirir
        console.log('[YT] Aktif yayın bulunamadı, 60s sonra tekrar deneniyor...');
        ytPollTimer = setTimeout(pollYouTubeChat, 60_000);
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
      // Round başlamadan önce atılmış mesajları sayma (önceki round'un emojileri karışmasın)
      const publishedAt = new Date(item.snippet?.publishedAt || 0).getTime();
      if (publishedAt < roundStartTime) continue;

      const text = item.snippet?.displayMessage || '';
      if (text.includes(TRIGGER_EMOJI)) {
        const userId = item.authorDetails.channelId;
        const displayName = item.authorDetails.displayName;
        recordHit('youtube', userId, displayName);
        console.log(`[YT] Hit: ${displayName}`);
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
  // ytChatId ve ytNextPageToken'ı KORUYORUZ:
  // aynı yayında sonraki round başlayınca chat'in kaldığı yerden devam ederiz,
  // eski mesajlar zaten publishedAt filtresiyle eleniyor.
}

// ── TikTok Live Chat ───────────────────────────────────────────────────────────
// Bağlantı sunucu açılır açılmaz kurulur ve yayın boyunca AÇIK kalır.
// Round başlangıcında bağlanma gecikmesi olmaz, ilk emojiler kaçmaz.
// Sayım zaten roundActive ile kontrol ediliyor.
let ttConnection = null;
let ttRetryDelay = 10_000; // yayın yokken kademeli artar (maks 2 dk), yayın açılınca sıfırlanır

function startTikTokChat() {
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
    const openedAt = { t: 0 };

    ws.on('open', () => {
      openedAt.t = Date.now();
      console.log('[TT] Bağlandı:', process.env.TIKTOK_USERNAME);
    });

    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        for (const evt of (msg.messages || [])) {
          if (evt.type !== 'WebcastChatMessage') continue;
          if (!roundActive) continue;
          const d = evt.data || {};
          const comment = d.comment || '';
          if (!comment.includes(TRIGGER_EMOJI)) continue;
          const user = d.user || {};
          // Sabit bir kimlik bul — yoksa sayma (rastgele ID dedup'u bozar)
          const userId = user.userId || user.openId || user.uniqueId || user.nickname;
          if (!userId) continue;
          const displayName = user.nickname || String(userId);
          recordHit('tiktok', `tt_${userId}`, displayName);
          console.log(`[TT] Hit: ${displayName} — "${comment}"`);
        }
      } catch (_) {}
    });

    ws.on('close', () => {
      ttConnection = null;
      // 1 dk'dan uzun açık kaldıysa yayın vardı → hızlı yeniden bağlan.
      // Hemen düştüyse yayın yok → bekleme süresini kademeli artır (maks 2 dk).
      if (openedAt.t && Date.now() - openedAt.t > 60_000) ttRetryDelay = 10_000;
      else ttRetryDelay = Math.min(ttRetryDelay * 2, 120_000);
      console.log(`[TT] Bağlantı kesildi, ${Math.round(ttRetryDelay / 1000)}s sonra yeniden deneniyor...`);
      setTimeout(connect, ttRetryDelay);
    });

    ws.on('error', (err) => {
      console.warn('[TT] WebSocket hatası:', err.message);
    });
  }

  connect();
}

// ── Round management ───────────────────────────────────────────────────────────
async function startRound(participantName, emoji, episode, location, youtubeVideo) {
  if (roundActive) {
    console.log('[Round] Zaten aktif round var, önce bitirin.');
    return { error: 'Zaten aktif bir oylama var' };
  }

  const triggerEmoji = emoji || DEFAULT_EMOJI;
  TRIGGER_EMOJI = triggerEmoji; // chat dinleyicisini güncelle

  // Host panelinden YouTube linki girildiyse kullan (kota dostu)
  const parsedVideoId = parseYouTubeVideoId(youtubeVideo);
  if (parsedVideoId && parsedVideoId !== ytVideoIdOverride) {
    ytVideoIdOverride = parsedVideoId;
    ytChatId = null; // yeni video → chat ID'yi yeniden bul
    ytNextPageToken = null;
    console.log('[YT] Video ID panelden alındı:', parsedVideoId);
  }

  const roundRef = db.ref('rounds').push();
  currentRoundId = roundRef.key;
  roundStartTime = Date.now();
  roundActive = true;

  // Bellekte sayım başlat
  pending = { participants: new Map(), total: 0, dirty: false };
  flushTimer = setInterval(flushRound, 1000);

  const roundData = {
    participantName: participantName || 'Katılımcı',
    emoji: triggerEmoji,
    episode: episode || '',
    location: location || '',
    startTime: roundStartTime,
    active: true,
    totalCount: 0,
    uniqueCount: 0,
    participants: {},
  };

  await roundRef.set(roundData);
  await db.ref('currentRound').set(currentRoundId);

  console.log(`\n[Round] BAŞLADI → ${participantName} | Emoji: ${triggerEmoji} | ID: ${currentRoundId}`);

  // Chat okumayı başlat (TikTok zaten sürekli bağlı)
  pollYouTubeChat();
  ttRetryDelay = 10_000; // round başlıyor → TikTok'a hızlı bağlanmayı dene
  startTikTokChat(); // bağlantı koptuysa güvence

  return { roundId: currentRoundId, participantName, emoji: triggerEmoji };
}

async function stopRound() {
  if (!roundActive || !currentRoundId) {
    return { error: 'Aktif oylama yok' };
  }

  roundActive = false; // önce sayımı durdur
  stopYouTubeChat();

  // Son sayıları bellekten al ve Firebase'e kesin yaz
  clearInterval(flushTimer);
  flushTimer = null;
  if (pending && pending.total > 0) pending.dirty = true;
  await flushRound();

  const uniqueCount = pending ? pending.participants.size : 0;
  const totalCount = pending ? pending.total : 0;

  const roundRef = db.ref(`rounds/${currentRoundId}`);
  await roundRef.update({ active: false, endTime: Date.now() });
  await db.ref('currentRound').remove();

  const snapshot = await roundRef.once('value');
  const result = snapshot.val() || {};

  console.log(`[Round] BİTTİ → Unique: ${uniqueCount} | Toplam: ${totalCount}`);

  const finishedId = currentRoundId;
  currentRoundId = null;
  pending = null;

  return {
    roundId: finishedId,
    participantName: result.participantName,
    emoji: result.emoji,
    uniqueCount,
    totalCount,
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
    tiktok: !!(ttConnection && ttConnection.readyState === WebSocket.OPEN),
  });
});

// Yayını açtıktan sonra beklemeden bağlanmak için (host paneldeki buton)
app.post('/tiktok/reconnect', (req, res) => {
  ttRetryDelay = 10_000;
  if (ttConnection) {
    try { ttConnection.close(); } catch (_) {}
    ttConnection = null;
  }
  startTikTokChat();
  res.json({ ok: true });
});

app.post('/round/start', async (req, res) => {
  const { participantName, emoji, episode, location, youtubeVideo } = req.body;
  const result = await startRound(participantName, emoji, episode, location, youtubeVideo);
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

  // TikTok'a hemen bağlan — round beklemeden hazır ol
  startTikTokChat();
});
