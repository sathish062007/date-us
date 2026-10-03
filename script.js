/**
 * ============================================================================
 * WEB BETWEEN US — Core Application Script
 * Technology: Pure Vanilla JavaScript (No frameworks, lightweight, clean)
 * Architecture: Clean separation of UI, Storage, Network, and Realtime Sync
 * ============================================================================
 */

(function () {
  'use strict';

  /* --------------------------------------------------------------------------
     1. APPLICATION STATE
     -------------------------------------------------------------------------- */
  const AppState = {
    user: null, // { id, name, email, isCreator }
    currentView: 'auth', // 'auth' | 'home' | 'watch' | 'chat' | 'settings'
    room: {
      code: null,
      isCreator: false,
      isConnected: false,
      partner: {
        id: null,
        name: 'Partner',
        isOnline: false,
        lastSeen: null
      },
      controllerMode: 'creator' // 'creator' (Room Creator Only) | 'both'
    },
    video: {
      file: null,
      objectUrl: null,
      fileName: '',
      duration: 0,
      currentTime: 0,
      isPlaying: false,
      isRemoteAction: false // Prevents synchronization ping-pong loops
    },
    network: {
      isOnline: navigator.onLine,
      isSimulatedOffline: false,
      status: navigator.onLine ? 'online' : 'offline' // 'online' | 'connecting' | 'weak' | 'offline'
    },
    chat: {
      messages: [],
      outgoingQueue: [],
      unreadCount: 0
    },
    settings: {
      darkMode: false,
      notifications: true,
      controllerMode: 'creator'
    }
  };

  /* --------------------------------------------------------------------------
     2. INDEXEDDB STORAGE MANAGER (Offline Persistence & Outgoing Queue)
     -------------------------------------------------------------------------- */
  const DB_NAME = 'WebBetweenUsDB';
  const DB_VERSION = 1;
  let dbInstance = null;

  const StorageManager = {
    async init() {
      return new Promise((resolve, reject) => {
        if (!window.indexedDB) {
          console.warn('[Storage] IndexedDB not supported. Falling back to localStorage.');
          resolve(false);
          return;
        }

        const request = window.indexedDB.open(DB_NAME, DB_VERSION);

        request.onupgradeneeded = (event) => {
          const db = event.target.result;
          // Store for chat messages
          if (!db.objectStoreNames.contains('chat_messages')) {
            const msgStore = db.createObjectStore('chat_messages', { keyPath: 'id' });
            msgStore.createIndex('roomCode', 'roomCode', { unique: false });
            msgStore.createIndex('timestamp', 'timestamp', { unique: false });
          }
          // Store for offline outgoing queue
          if (!db.objectStoreNames.contains('outgoing_queue')) {
            db.createObjectStore('outgoing_queue', { keyPath: 'id' });
          }
        };

        request.onsuccess = (event) => {
          dbInstance = event.target.result;
          console.log('[Storage] IndexedDB initialized successfully.');
          resolve(true);
        };

        request.onerror = (event) => {
          console.error('[Storage] IndexedDB error:', event.target.error);
          resolve(false);
        };
      });
    },

    async saveMessage(message) {
      if (!dbInstance) {
        // LocalStorage fallback
        const local = JSON.parse(localStorage.getItem('wbu_chat_backup') || '[]');
        local.push(message);
        localStorage.setItem('wbu_chat_backup', JSON.stringify(local.slice(-100)));
        return;
      }
      return new Promise((resolve) => {
        try {
          const tx = dbInstance.transaction('chat_messages', 'readwrite');
          const store = tx.objectStore('chat_messages');
          store.put(message);
          tx.oncomplete = () => resolve(true);
          tx.onerror = () => resolve(false);
        } catch (e) {
          console.error('[Storage] Error saving message:', e);
          resolve(false);
        }
      });
    },

    async loadMessages(roomCode) {
      if (!dbInstance) {
        return JSON.parse(localStorage.getItem('wbu_chat_backup') || '[]');
      }
      return new Promise((resolve) => {
        try {
          const tx = dbInstance.transaction('chat_messages', 'readonly');
          const store = tx.objectStore('chat_messages');
          const request = store.getAll();
          request.onsuccess = () => {
            const all = request.result || [];
            const filtered = roomCode ? all.filter(m => m.roomCode === roomCode) : all;
            resolve(filtered.sort((a, b) => a.timestamp - b.timestamp));
          };
          request.onerror = () => resolve([]);
        } catch (e) {
          resolve([]);
        }
      });
    },

    async enqueueOffline(item) {
      if (!dbInstance) return;
      return new Promise((resolve) => {
        try {
          const tx = dbInstance.transaction('outgoing_queue', 'readwrite');
          tx.objectStore('outgoing_queue').put(item);
          tx.oncomplete = () => resolve(true);
          tx.onerror = () => resolve(false);
        } catch (e) {
          resolve(false);
        }
      });
    },

    async getOfflineQueue() {
      if (!dbInstance) return [];
      return new Promise((resolve) => {
        try {
          const tx = dbInstance.transaction('outgoing_queue', 'readonly');
          const req = tx.objectStore('outgoing_queue').getAll();
          req.onsuccess = () => resolve(req.result || []);
          req.onerror = () => resolve([]);
        } catch (e) {
          resolve([]);
        }
      });
    },

    async removeOfflineItem(id) {
      if (!dbInstance) return;
      return new Promise((resolve) => {
        try {
          const tx = dbInstance.transaction('outgoing_queue', 'readwrite');
          tx.objectStore('outgoing_queue').delete(id);
          tx.oncomplete = () => resolve(true);
        } catch (e) {
          resolve(false);
        }
      });
    },

    async clearAllChat() {
      if (dbInstance) {
        const tx = dbInstance.transaction(['chat_messages', 'outgoing_queue'], 'readwrite');
        tx.objectStore('chat_messages').clear();
        tx.objectStore('outgoing_queue').clear();
      }
      localStorage.removeItem('wbu_chat_backup');
    }
  };

  /* --------------------------------------------------------------------------
     3. REALTIME COMMUNICATION & MULTI-TAB SIMULATION LAYER
     --------------------------------------------------------------------------
     Uses BroadcastChannel (with localStorage fallback) so two tabs/windows
     on the same computer can test synchronized playback and chat in real-time.
     This layer is cleanly separated and ready to swap for WebSocket/WebRTC/Backend.
     -------------------------------------------------------------------------- */
  let syncChannel = null;

  const RealtimeLayer = {
    init() {
      try {
        if ('BroadcastChannel' in window) {
          syncChannel = new BroadcastChannel('web_between_us_sync_channel');
          syncChannel.onmessage = (event) => {
            this.handleIncomingRaw(event.data);
          };
        } else {
          // Fallback to storage event
          window.addEventListener('storage', (e) => {
            if (e.key === 'wbu_cross_tab_event' && e.newValue) {
              try {
                const data = JSON.parse(e.newValue);
                this.handleIncomingRaw(data);
              } catch (err) {}
            }
          });
        }
      } catch (err) {
        console.warn('[Realtime] BroadcastChannel init error:', err);
      }
    },

    sendRaw(payload) {
      // Don't send if offline or simulated offline
      if (!AppState.network.isOnline || AppState.network.isSimulatedOffline) {
        console.log('[Realtime] Offline: message queued locally.');
        return false;
      }

      // 1. BroadcastChannel
      if (syncChannel) {
        try {
          syncChannel.postMessage(payload);
        } catch (e) {
          console.error('[Realtime] BroadcastChannel postMessage error:', e);
        }
      }

      // 2. Storage event fallback for cross-tab
      try {
        localStorage.setItem('wbu_cross_tab_event', JSON.stringify({
          ...payload,
          _nonce: Date.now() + Math.random()
        }));
      } catch (e) {}

      return true;
    },

    handleIncomingRaw(payload) {
      if (!payload || typeof payload !== 'object') return;
      if (!AppState.room.code) return; // Not in a room
      if (payload.roomCode !== AppState.room.code) return; // Different room
      if (payload.senderId === (AppState.user && AppState.user.id)) return; // Ignore own messages

      // Route event
      receiveRoomEvent(payload);
    }
  };

  /* --------------------------------------------------------------------------
     4. EXPLICIT CONNECTION LAYER FUNCTIONS (As requested in specification)
     -------------------------------------------------------------------------- */

  /**
   * Generates a new unique room code and creates a private room.
   */
  window.createRoom = function () {
    const randomNum = Math.floor(10 + Math.random() * 90);
    const words = ['LOVE', 'DEAR', 'PAIR', 'MOMENT', 'HEART', 'FOREVER', 'STARS'];
    const randomWord = words[Math.floor(Math.random() * words.length)];
    const code = `WB${randomNum}-${randomWord}`;

    AppState.room.code = code;
    AppState.room.isCreator = true;
    AppState.room.isConnected = true;
    AppState.room.partner.isOnline = false;

    // Save active room in session
    sessionStorage.setItem('wbu_active_room', JSON.stringify({
      code,
      isCreator: true
    }));

    // Broadcast room created presence ping
    sendRoomEvent({
      type: 'ROOM_CREATED',
      creatorName: AppState.user.name
    });

    return code;
  };

  /**
   * Joins an existing room by room code.
   */
  window.joinRoom = function (roomCode) {
    if (!roomCode) return Promise.reject(new Error("Room code isn't valid. Try again."));

    const cleanedCode = roomCode.trim().toUpperCase();
    if (cleanedCode.length < 4) {
      return Promise.reject(new Error("Room code isn't valid. Try again."));
    }

    return new Promise((resolve, reject) => {
      // Simulate network connection attempt
      setTimeout(() => {
        if (!AppState.network.isOnline || AppState.network.isSimulatedOffline) {
          reject(new Error("You're offline. Please check your network connection."));
          return;
        }

        AppState.room.code = cleanedCode;
        AppState.room.isCreator = false;
        AppState.room.isConnected = true;
        AppState.room.partner.isOnline = true; // Connected to creator
        AppState.room.partner.name = 'Partner';

        sessionStorage.setItem('wbu_active_room', JSON.stringify({
          code: cleanedCode,
          isCreator: false
        }));

        // Send JOINED event to creator
        sendRoomEvent({
          type: 'USER_JOINED',
          partnerName: AppState.user.name
        });

        resolve({ code: cleanedCode });
      }, 500);
    });
  };

  /**
   * Connects to a room session.
   */
  window.connectToRoom = function (roomCode) {
    AppState.room.code = roomCode;
    AppState.room.isConnected = true;
    updateRoomUI();
  };

  /**
   * Disconnects and leaves the current room.
   */
  window.disconnectFromRoom = function () {
    if (AppState.room.code) {
      sendRoomEvent({
        type: 'USER_LEFT',
        userName: AppState.user ? AppState.user.name : 'User'
      });
    }

    AppState.room.code = null;
    AppState.room.isCreator = false;
    AppState.room.isConnected = false;
    AppState.room.partner.isOnline = false;
    sessionStorage.removeItem('wbu_active_room');

    updateRoomUI();
    UIManager.showToast('You left the room.');
    UIManager.switchView('home');
  };

  /**
   * Sends a generic room event over the realtime layer.
   */
  window.sendRoomEvent = function (event) {
    const payload = {
      ...event,
      roomCode: AppState.room.code,
      senderId: AppState.user ? AppState.user.id : 'guest',
      senderName: AppState.user ? AppState.user.name : 'Partner',
      timestamp: Date.now()
    };
    return RealtimeLayer.sendRaw(payload);
  };

  const DEMO_MOVIE_URL = 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4';

  /**
   * Receives and dispatches incoming room events.
   */
  window.receiveRoomEvent = function (event) {
    console.log('[Room Event Received]:', event.type, event);

    switch (event.type) {
      case 'USER_JOINED':
        AppState.room.partner.isOnline = true;
        if (event.partnerName) {
          AppState.room.partner.name = event.partnerName;
        }
        updatePartnerPresenceUI(true);
        updateRoomUI();

        // Close any open modals and immediately bring User A to the Watch room screen!
        UIManager.closeModal('modal-create-room');
        UIManager.closeModal('modal-join-room');
        UIManager.switchView('watch');

        UIManager.showToast(`❤️ ${AppState.room.partner.name} joined your room!`);

        // Send confirmation Pong back to Partner with our user info and video state
        sendRoomEvent({
          type: 'PRESENCE_PONG',
          creatorName: AppState.user ? AppState.user.name : 'Creator',
          videoLoaded: !!(videoEl && videoEl.src),
          videoTime: videoEl ? videoEl.currentTime : 0,
          isPlaying: videoEl ? !videoEl.paused : false,
          sampleUrl: AppState.video.sampleUrl || null,
          fileName: AppState.video.fileName || ''
        });
        break;

      case 'PRESENCE_PONG':
        AppState.room.partner.isOnline = true;
        if (event.creatorName) {
          AppState.room.partner.name = event.creatorName;
        }
        updatePartnerPresenceUI(true);
        updateRoomUI();

        // If partner has loaded a demo clip, sync load it automatically
        if (event.sampleUrl && (!videoEl || !videoEl.src)) {
          VideoController.loadDemoMovie(false);
          if (event.isPlaying) {
            setTimeout(() => {
              if (videoEl) {
                videoEl.currentTime = event.videoTime || 0;
                videoEl.play().catch(e => {});
              }
            }, 500);
          }
        } else if (event.fileName) {
          showPartnerVideoNotice(`Partner loaded: ${event.fileName}`);
        }
        break;

      case 'MEDIA_LOADED':
        if (event.sampleUrl && (!videoEl || !videoEl.src)) {
          VideoController.loadDemoMovie(false);
        } else if (event.fileName) {
          showPartnerVideoNotice(`Partner loaded: ${event.fileName}`);
        }
        UIManager.showToast(`🎬 Partner loaded: ${event.fileName || 'Movie'}`);
        break;

      case 'USER_LEFT':
        AppState.room.partner.isOnline = false;
        updatePartnerPresenceUI(false);
        UIManager.showToast(`${AppState.room.partner.name || 'Partner'} left the room.`);
        break;

      case 'PLAYBACK_STATE':
        receivePlaybackState(event);
        break;

      case 'CHAT_MESSAGE':
        receiveChatMessage(event.message);
        break;

      case 'CONTROLLER_MODE_CHANGE':
        AppState.room.controllerMode = event.mode;
        updateControllerModeUI();
        UIManager.showToast(`Controller mode changed to: ${event.mode === 'both' ? 'Both Control' : 'Room Creator Only'}`);
        break;

      default:
        break;
    }
  };

  /**
   * Sends lightweight video playback state.
   */
  window.sendPlaybackState = function (state) {
    const payload = {
      type: 'PLAYBACK_STATE',
      action: state.action, // 'PLAY' | 'PAUSE' | 'SEEK' | 'SYNC'
      currentTime: state.currentTime,
      isPlaying: state.isPlaying,
      mediaTitle: AppState.video.fileName,
      sampleUrl: AppState.video.sampleUrl || null
    };
    sendRoomEvent(payload);
  };

  /**
   * Receives remote video playback state and applies it.
   */
  window.receivePlaybackState = function (state) {
    receivePlaybackUpdate(state);
  };

  /**
   * Sends a chat message with offline queue support.
   */
  window.sendChatMessage = async function (text) {
    if (!text || !text.trim()) return;

    const message = {
      id: 'msg_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5),
      roomCode: AppState.room.code || 'local',
      senderId: AppState.user ? AppState.user.id : 'user',
      senderName: AppState.user ? AppState.user.name : 'You',
      text: text.trim(),
      timestamp: Date.now(),
      status: 'sent' // 'queued' | 'sending' | 'sent'
    };

    // Update quick preview in Watch view
    const previewEl = document.getElementById('watch-recent-message');
    if (previewEl) {
      previewEl.innerHTML = `<span class="msg-sender">You:</span> <span class="msg-text">${escapeHTML(message.text)}</span>`;
    }

    const isOffline = !AppState.network.isOnline || AppState.network.isSimulatedOffline;

    if (isOffline) {
      message.status = 'queued';
      // Save to IndexedDB
      await StorageManager.saveMessage(message);
      await StorageManager.enqueueOffline(message);
      AppState.chat.messages.push(message);
      ChatUI.appendBubble(message, true);
      ChatUI.updateSyncStatus();
      UIManager.showToast('Message saved offline. Will send when connected.');
      return;
    }

    // Online: Save and send
    message.status = 'sent';
    await StorageManager.saveMessage(message);
    AppState.chat.messages.push(message);
    ChatUI.appendBubble(message, true);

    sendRoomEvent({
      type: 'CHAT_MESSAGE',
      message
    });
  };

  /**
   * Receives incoming partner chat message.
   */
  window.receiveChatMessage = async function (message) {
    if (!message) return;
    await StorageManager.saveMessage(message);
    AppState.chat.messages.push(message);
    ChatUI.appendBubble(message, false);

    // Update quick preview in Watch view
    const previewEl = document.getElementById('watch-recent-message');
    if (previewEl) {
      previewEl.innerHTML = `<span class="msg-sender">${escapeHTML(message.senderName)}:</span> <span class="msg-text">${escapeHTML(message.text)}</span>`;
    }

    // Show floating reaction toast over video if on Watch screen
    if (AppState.currentView === 'watch') {
      showVideoOverlayToast(`💬 ${message.senderName}: ${message.text.substring(0, 35)}`);
    }

    // Badge notification if not on chat screen
    if (AppState.currentView !== 'chat') {
      AppState.chat.unreadCount++;
      const badge = document.getElementById('chat-nav-badge');
      if (badge) {
        badge.textContent = AppState.chat.unreadCount;
        badge.classList.remove('hidden');
      }
      if (AppState.settings.notifications) {
        UIManager.showToast(`💬 ${message.senderName}: ${message.text.substring(0, 30)}`);
      }
    }
  };

  /* --------------------------------------------------------------------------
     5. SYNCHRONIZED VIDEO PLAYBACK CONTROLLER
     -------------------------------------------------------------------------- */
  const videoEl = document.getElementById('main-video');
  const SYNC_DRIFT_TOLERANCE = 0.8; // Seconds of difference before correcting

  window.playTogether = function () {
    if (!checkCanControl()) return;
    if (videoEl && videoEl.src) {
      videoEl.play().catch(e => console.log('Local play intercepted:', e));
      sendPlaybackState({
        action: 'PLAY',
        currentTime: videoEl.currentTime,
        isPlaying: true
      });
    }
  };

  window.pauseTogether = function () {
    if (!checkCanControl()) return;
    if (videoEl && videoEl.src) {
      videoEl.pause();
      sendPlaybackState({
        action: 'PAUSE',
        currentTime: videoEl.currentTime,
        isPlaying: false
      });
    }
  };

  window.seekTogether = function (targetTime) {
    if (!checkCanControl()) return;
    if (videoEl && videoEl.src) {
      videoEl.currentTime = targetTime;
      sendPlaybackState({
        action: 'SEEK',
        currentTime: targetTime,
        isPlaying: !videoEl.paused
      });
    }
  };

  window.syncPlayback = function () {
    if (videoEl && videoEl.src && !videoEl.paused) {
      sendPlaybackState({
        action: 'SYNC',
        currentTime: videoEl.currentTime,
        isPlaying: true
      });
    }
  };

  /**
   * Handles incoming remote playback updates without causing recursive loops.
   */
  window.receivePlaybackUpdate = function (state) {
    if (!state) return;

    // Ensure the video player UI is visible on this device
    if (!videoEl || !videoEl.src) {
      VideoController.loadDemoMovie(false, state.mediaTitle || 'Between Us Movie');
    } else {
      document.getElementById('video-empty-state').classList.add('hidden');
      document.getElementById('custom-video-controls').classList.remove('hidden');
      videoEl.classList.remove('hidden');
    }

    // Set loop prevention flag
    AppState.video.isRemoteAction = true;

    try {
      const timeDiff = Math.abs((videoEl ? videoEl.currentTime : 0) - (state.currentTime || 0));

      if (state.action === 'PLAY') {
        if (videoEl) {
          if (timeDiff > 0.3) {
            try { videoEl.currentTime = state.currentTime; } catch (e) {}
          }
          // Attempt playback with fallback to muted autoplay if needed
          const playPromise = videoEl.play();
          if (playPromise !== undefined) {
            playPromise.catch(() => {
              // Autoplay policy fallback: mute and play so video definitely shows
              videoEl.muted = true;
              const volIcon = document.getElementById('ctrl-volume-icon');
              if (volIcon) volIcon.textContent = '🔇';
              videoEl.play().catch(e => console.log('Remote play policy:', e));
              showVideoOverlayToast('▶ Playing (Tap 🔊 to unmute)');
            });
          }
        }
        showVideoOverlayToast('▶ Partner played');
      } else if (state.action === 'PAUSE') {
        if (videoEl) {
          if (timeDiff > 0.3) {
            try { videoEl.currentTime = state.currentTime; } catch (e) {}
          }
          videoEl.pause();
        }
        showVideoOverlayToast('⏸ Partner paused');
      } else if (state.action === 'SEEK') {
        if (videoEl) {
          try { videoEl.currentTime = state.currentTime; } catch (e) {}
          if (state.isPlaying) {
            videoEl.play().catch(() => {
              videoEl.muted = true;
              videoEl.play().catch(e => {});
            });
          }
        }
        showVideoOverlayToast(`⏩ Partner sought to ${formatTime(state.currentTime)}`);
      } else if (state.action === 'SYNC') {
        if (videoEl && timeDiff > SYNC_DRIFT_TOLERANCE) {
          try { videoEl.currentTime = state.currentTime; } catch (e) {}
        }
      }
    } finally {
      setTimeout(() => {
        AppState.video.isRemoteAction = false;
      }, 350);
    }
  };

  function checkCanControl() {
    if (AppState.room.controllerMode === 'creator' && !AppState.room.isCreator) {
      const creatorName = AppState.room.partner.name || 'Room Creator';
      UIManager.showToast(`🔒 Only ${creatorName} can control video playback.`);
      showVideoOverlayToast(`🔒 ${creatorName} controls playback`);
      return false;
    }
    return true;
  }

  function showVideoOverlayToast(message) {
    const toast = document.getElementById('video-overlay-toast');
    const msgSpan = document.getElementById('video-toast-msg');
    if (!toast || !msgSpan) return;

    msgSpan.textContent = message;
    toast.classList.remove('hidden');

    clearTimeout(toast._timeout);
    toast._timeout = setTimeout(() => {
      toast.classList.add('hidden');
    }, 2500);
  }

  /* --------------------------------------------------------------------------
     6. NETWORK MONITOR & OFFLINE QUEUE PROCESSOR
     -------------------------------------------------------------------------- */
  const NetworkManager = {
    init() {
      window.addEventListener('online', () => this.handleNetworkChange(true));
      window.addEventListener('offline', () => this.handleNetworkChange(false));
      this.updateStatusDisplay();

      // Periodic queue check
      setInterval(() => {
        if (AppState.network.isOnline && !AppState.network.isSimulatedOffline) {
          this.drainOutgoingQueue();
        }
      }, 3000);
    },

    handleNetworkChange(isOnline) {
      AppState.network.isOnline = isOnline;
      this.updateStatusDisplay();

      const banner = document.getElementById('network-banner');
      const bannerMsg = document.getElementById('banner-message');

      if (!isOnline || AppState.network.isSimulatedOffline) {
        if (banner && bannerMsg) {
          bannerMsg.textContent = "You're offline. Local video and offline chat are still available.";
          banner.className = 'network-banner';
          banner.classList.remove('hidden');
        }
        UIManager.showToast('🔴 Offline Mode active');
      } else {
        if (banner && bannerMsg) {
          bannerMsg.textContent = "Back online! Syncing data…";
          banner.className = 'network-banner online-toast';
          banner.classList.remove('hidden');
          setTimeout(() => banner.classList.add('hidden'), 2500);
        }
        UIManager.showToast('🟢 Back online');
        this.drainOutgoingQueue();
      }
    },

    toggleSimulatedOffline() {
      AppState.network.isSimulatedOffline = !AppState.network.isSimulatedOffline;
      const btn = document.getElementById('btn-toggle-sim-offline');
      if (btn) {
        btn.textContent = AppState.network.isSimulatedOffline ? 'Resume Online Mode' : 'Simulate Network Offline';
        btn.classList.toggle('btn-danger', AppState.network.isSimulatedOffline);
      }
      this.handleNetworkChange(!AppState.network.isSimulatedOffline);
    },

    updateStatusDisplay() {
      const isConnected = AppState.network.isOnline && !AppState.network.isSimulatedOffline;
      const pill = document.getElementById('connection-pill');
      const text = document.getElementById('connection-text');
      const chip = document.getElementById('network-chip-state');
      const watchOffline = document.getElementById('watch-offline-badge');

      if (pill && text) {
        pill.className = `status-pill ${isConnected ? 'online' : 'offline'}`;
        text.textContent = isConnected ? 'Online' : 'Offline';
      }
      if (chip) {
        chip.textContent = isConnected ? '🟢 Online' : '🔴 Offline';
      }
      if (watchOffline) {
        watchOffline.style.display = isConnected ? 'none' : 'block';
      }
    },

    async drainOutgoingQueue() {
      const queue = await StorageManager.getOfflineQueue();
      if (!queue || queue.length === 0) return;

      console.log(`[Queue] Draining ${queue.length} offline message(s)...`);
      for (const item of queue) {
        item.status = 'sent';
        await StorageManager.saveMessage(item);
        await StorageManager.removeOfflineItem(item.id);

        sendRoomEvent({
          type: 'CHAT_MESSAGE',
          message: item
        });
      }
      ChatUI.refreshMessages();
      ChatUI.updateSyncStatus();
    }
  };

  /* --------------------------------------------------------------------------
     7. VIDEO PLAYER & FILE SELECTION LOGIC
     -------------------------------------------------------------------------- */
  const VideoController = {
    init() {
      const fileInput = document.getElementById('video-file-input');
      const fileInputChange = document.getElementById('video-file-input-change');

      if (fileInput) fileInput.addEventListener('change', (e) => this.handleFileSelection(e));
      if (fileInputChange) fileInputChange.addEventListener('change', (e) => this.handleFileSelection(e));

      // Custom Video Control Buttons
      const btnPlay = document.getElementById('btn-video-play-pause');
      const btnRewind = document.getElementById('btn-video-rewind');
      const btnForward = document.getElementById('btn-video-forward');
      const btnMute = document.getElementById('btn-video-mute');
      const btnFullscreen = document.getElementById('btn-video-fullscreen');
      const progressContainer = document.getElementById('video-progress-container');

      if (btnPlay) {
        btnPlay.addEventListener('click', () => {
          if (!videoEl.src) return;
          if (videoEl.paused) {
            playTogether();
          } else {
            pauseTogether();
          }
        });
      }

      if (btnRewind) {
        btnRewind.addEventListener('click', () => {
          if (!videoEl.src) return;
          seekTogether(Math.max(0, videoEl.currentTime - 10));
        });
      }

      if (btnForward) {
        btnForward.addEventListener('click', () => {
          if (!videoEl.src) return;
          seekTogether(Math.min(videoEl.duration || 0, videoEl.currentTime + 10));
        });
      }

      if (btnMute) {
        btnMute.addEventListener('click', () => {
          videoEl.muted = !videoEl.muted;
          const volIcon = document.getElementById('ctrl-volume-icon');
          if (volIcon) volIcon.textContent = videoEl.muted ? '🔇' : '🔊';
        });
      }

      if (btnFullscreen) {
        btnFullscreen.addEventListener('click', () => {
          const wrapper = document.querySelector('.video-player-wrapper');
          if (!document.fullscreenElement) {
            if (wrapper.requestFullscreen) wrapper.requestFullscreen();
            else if (videoEl.webkitEnterFullscreen) videoEl.webkitEnterFullscreen();
          } else {
            if (document.exitFullscreen) document.exitFullscreen();
          }
        });
      }

      // Progress Scrubbing
      if (progressContainer) {
        let isDragging = false;
        const seekFromEvent = (e) => {
          if (!videoEl.duration) return;
          const rect = progressContainer.getBoundingClientRect();
          const clientX = e.clientX || (e.touches && e.touches[0].clientX) || 0;
          const pos = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
          seekTogether(pos * videoEl.duration);
        };

        progressContainer.addEventListener('mousedown', (e) => {
          isDragging = true;
          seekFromEvent(e);
        });

        window.addEventListener('mousemove', (e) => {
          if (isDragging) seekFromEvent(e);
        });

        window.addEventListener('mouseup', () => {
          isDragging = false;
        });

        // Touch scrubbing for mobile
        progressContainer.addEventListener('touchstart', (e) => {
          isDragging = true;
          seekFromEvent(e);
        }, { passive: true });

        progressContainer.addEventListener('touchmove', (e) => {
          if (isDragging) seekFromEvent(e);
        }, { passive: true });

        progressContainer.addEventListener('touchend', () => {
          isDragging = false;
        });
      }

      // Video Event Listeners
      videoEl.addEventListener('play', () => {
        const playIcon = document.getElementById('ctrl-play-icon');
        if (playIcon) playIcon.textContent = '⏸';
        if (!AppState.video.isRemoteAction) {
          sendPlaybackState({
            action: 'PLAY',
            currentTime: videoEl.currentTime,
            isPlaying: true
          });
        }
      });

      videoEl.addEventListener('pause', () => {
        const playIcon = document.getElementById('ctrl-play-icon');
        if (playIcon) playIcon.textContent = '▶';
        if (!AppState.video.isRemoteAction) {
          sendPlaybackState({
            action: 'PAUSE',
            currentTime: videoEl.currentTime,
            isPlaying: false
          });
        }
      });

      videoEl.addEventListener('timeupdate', () => {
        this.updateProgress();
      });

      videoEl.addEventListener('loadedmetadata', () => {
        AppState.video.duration = videoEl.duration;
        const durText = document.getElementById('duration-time-text');
        if (durText) durText.textContent = formatTime(videoEl.duration);
      });

      const btnSample = document.getElementById('btn-load-sample-movie');
      if (btnSample) {
        btnSample.addEventListener('click', () => this.loadDemoMovie(true));
      }

      videoEl.addEventListener('error', () => {
        UIManager.showToast("This video format isn't supported by your browser.", 'danger');
      });

      // Periodic lightweight heartbeat sync (every 5 seconds when playing)
      setInterval(() => {
        if (!videoEl.paused && videoEl.src && AppState.room.isConnected) {
          syncPlayback();
        }
      }, 5000);
    },

    loadDemoMovie(broadcast = true, titleOverride = null) {
      if (AppState.video.objectUrl) {
        URL.revokeObjectURL(AppState.video.objectUrl);
        AppState.video.objectUrl = null;
      }

      const movieTitle = titleOverride || 'Between Us - Romantic Demo Clip.mp4';
      AppState.video.file = null;
      AppState.video.fileName = movieTitle;
      AppState.video.sampleUrl = DEMO_MOVIE_URL;

      videoEl.src = DEMO_MOVIE_URL;
      videoEl.load();

      // Update UI
      document.getElementById('video-empty-state').classList.add('hidden');
      document.getElementById('custom-video-controls').classList.remove('hidden');
      videoEl.classList.remove('hidden');

      const titleEl = document.getElementById('now-playing-title');
      if (titleEl) titleEl.textContent = movieTitle;

      if (broadcast) {
        sendRoomEvent({
          type: 'MEDIA_LOADED',
          fileName: movieTitle,
          sampleUrl: DEMO_MOVIE_URL
        });
        UIManager.showToast('🎬 Loaded Romantic Demo Clip! Both players ready.');
      }
    },

    handleFileSelection(event) {
      const file = event.target.files && event.target.files[0];
      if (!file) return;

      // Clean up previous Object URL to prevent memory leaks on mobile!
      if (AppState.video.objectUrl) {
        URL.revokeObjectURL(AppState.video.objectUrl);
      }

      AppState.video.file = file;
      AppState.video.fileName = file.name;
      AppState.video.sampleUrl = null;
      AppState.video.objectUrl = URL.createObjectURL(file);

      // Load into video element
      videoEl.src = AppState.video.objectUrl;
      videoEl.load();

      // Update UI
      document.getElementById('video-empty-state').classList.add('hidden');
      document.getElementById('custom-video-controls').classList.remove('hidden');
      videoEl.classList.remove('hidden');

      const titleEl = document.getElementById('now-playing-title');
      if (titleEl) titleEl.textContent = file.name;

      sendRoomEvent({
        type: 'MEDIA_LOADED',
        fileName: file.name,
        sampleUrl: null
      });

      UIManager.showToast(`🎬 Loaded local movie: ${file.name}`);
    },

    updateProgress() {
      if (!videoEl.duration) return;
      const progressPercent = (videoEl.currentTime / videoEl.duration) * 100;
      
      const playedBar = document.getElementById('video-progress-bar');
      const scrubber = document.getElementById('video-scrubber-handle');
      const currTimeText = document.getElementById('current-time-text');

      if (playedBar) playedBar.style.width = `${progressPercent}%`;
      if (scrubber) scrubber.style.left = `${progressPercent}%`;
      if (currTimeText) currTimeText.textContent = formatTime(videoEl.currentTime);

      // Update buffered progress
      if (videoEl.buffered.length > 0) {
        const bufferedEnd = videoEl.buffered.end(videoEl.buffered.length - 1);
        const bufferedPercent = (bufferedEnd / videoEl.duration) * 100;
        const buffBar = document.getElementById('video-buffered-bar');
        if (buffBar) buffBar.style.width = `${bufferedPercent}%`;
      }
    }
  };

  function showPartnerVideoNotice(msg) {
    const notice = document.getElementById('partner-video-notice');
    const noticeText = document.getElementById('partner-video-notice-text');
    if (notice && noticeText) {
      noticeText.textContent = msg;
      notice.classList.remove('hidden');
    }
  }

  /* --------------------------------------------------------------------------
     8. CHAT INTERFACE & EMOJI PICKER
     -------------------------------------------------------------------------- */
  const ChatUI = {
    init() {
      const btnSend = document.getElementById('btn-send-chat');
      const textInput = document.getElementById('chat-text-input');
      const btnEmoji = document.getElementById('btn-toggle-emoji');
      const btnCloseEmoji = document.getElementById('btn-close-emoji');
      const emojiGrid = document.getElementById('emoji-grid');

      if (btnSend && textInput) {
        btnSend.addEventListener('click', () => this.submitMessage());
        textInput.addEventListener('keydown', (e) => {
          if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            this.submitMessage();
          }
        });
      }

      if (btnEmoji) {
        btnEmoji.addEventListener('click', () => {
          const container = document.getElementById('emoji-picker-container');
          if (container) container.classList.toggle('hidden');
        });
      }

      if (btnCloseEmoji) {
        btnCloseEmoji.addEventListener('click', () => {
          document.getElementById('emoji-picker-container').classList.add('hidden');
        });
      }

      if (emojiGrid && textInput) {
        emojiGrid.addEventListener('click', (e) => {
          const btn = e.target.closest('.emoji-btn');
          if (!btn) return;
          const emoji = btn.textContent;
          this.insertEmojiAtCursor(textInput, emoji);
        });
      }

      // Quick chat shortcut in watch view
      const btnJumpChat = document.getElementById('btn-jump-to-chat');
      if (btnJumpChat) {
        btnJumpChat.addEventListener('click', () => UIManager.switchView('chat'));
      }

      // Inline watch chat input and reaction chips
      const watchInput = document.getElementById('watch-quick-chat-input');
      const btnWatchSend = document.getElementById('btn-watch-quick-send');
      if (watchInput && btnWatchSend) {
        const sendWatchMsg = () => {
          const val = watchInput.value;
          if (val && val.trim()) {
            sendChatMessage(val.trim());
            watchInput.value = '';
          }
        };
        btnWatchSend.addEventListener('click', sendWatchMsg);
        watchInput.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            sendWatchMsg();
          }
        });
      }

      // Reaction chips
      document.querySelectorAll('.reaction-chip').forEach(chip => {
        chip.addEventListener('click', () => {
          const emoji = chip.getAttribute('data-emoji') || chip.textContent.trim();
          sendChatMessage(emoji);
          UIManager.showToast(`Sent reaction ${emoji}`);
        });
      });
    },

    insertEmojiAtCursor(input, emoji) {
      const start = input.selectionStart || input.value.length;
      const end = input.selectionEnd || input.value.length;
      const text = input.value;
      input.value = text.substring(0, start) + emoji + text.substring(end);
      input.selectionStart = input.selectionEnd = start + emoji.length;
      input.focus();
    },

    submitMessage() {
      const textInput = document.getElementById('chat-text-input');
      if (!textInput) return;
      const text = textInput.value;
      if (!text || !text.trim()) return;

      sendChatMessage(text);
      textInput.value = '';
      textInput.style.height = 'auto';
    },

    appendBubble(message, isSelf) {
      const area = document.getElementById('chat-messages-area');
      if (!area) return;

      const wrapper = document.createElement('div');
      wrapper.className = `chat-bubble-wrapper ${isSelf ? 'you' : 'partner'}`;
      wrapper.id = `bubble-${message.id}`;

      const timeStr = formatShortTime(message.timestamp || Date.now());
      let statusIcon = '';
      if (isSelf) {
        if (message.status === 'queued') {
          statusIcon = '<span class="msg-status-icon queued" title="Waiting to send">⏱ Waiting to send</span>';
        } else if (message.status === 'sending') {
          statusIcon = '<span class="msg-status-icon queued" title="Syncing">🔄 Syncing…</span>';
        } else {
          statusIcon = '<span class="msg-status-icon sent" title="Sent">Sent ✓</span>';
        }
      }

      wrapper.innerHTML = `
        <div class="chat-bubble">
          ${escapeHTML(message.text)}
        </div>
        <div class="chat-bubble-meta">
          <span>${timeStr}</span>
          ${statusIcon}
        </div>
      `;

      area.appendChild(wrapper);
      area.scrollTop = area.scrollHeight;
    },

    async refreshMessages() {
      const area = document.getElementById('chat-messages-area');
      if (!area) return;

      const messages = await StorageManager.loadMessages(AppState.room.code);
      AppState.chat.messages = messages;

      area.innerHTML = '<div class="chat-date-divider"><span>Today</span></div>';
      const myId = AppState.user ? AppState.user.id : 'user';

      messages.forEach(msg => {
        const isSelf = msg.senderId === myId || msg.senderName === (AppState.user && AppState.user.name);
        this.appendBubble(msg, isSelf);
      });
    },

    updateSyncStatus() {
      const pill = document.getElementById('chat-sync-status');
      if (!pill) return;

      const hasQueued = AppState.chat.messages.some(m => m.status === 'queued');
      if (hasQueued) {
        pill.textContent = 'Queued offline ⏱';
        pill.style.color = 'var(--status-weak)';
      } else {
        pill.textContent = 'All synced ✓';
        pill.style.color = 'var(--text-secondary)';
      }
    }
  };

  /* --------------------------------------------------------------------------
     9. UI MANAGER & VIEW ROUTER
     -------------------------------------------------------------------------- */
  const UIManager = {
    init() {
      // Bottom Navigation Click Handlers
      document.querySelectorAll('.nav-tab').forEach(tab => {
        tab.addEventListener('click', () => {
          const targetView = tab.getAttribute('data-view');
          if (targetView) this.switchView(targetView);
        });
      });

      // Quick Shortcuts on Home Screen
      this.bindClick('card-create-room', () => this.openCreateRoomModal());
      this.bindClick('card-join-room', () => this.openJoinRoomModal());
      this.bindClick('nav-shortcut-watch', () => this.switchView('watch'));
      this.bindClick('nav-shortcut-chat', () => this.switchView('chat'));
      this.bindClick('nav-shortcut-offline', () => {
        this.switchView('watch');
        UIManager.showToast('Offline Mode: Choose any local video to play!');
      });
      this.bindClick('nav-shortcut-settings', () => this.switchView('settings'));

      this.bindClick('btn-home-enter-room', () => this.switchView('watch'));
      this.bindClick('btn-home-leave-room', () => this.confirmLeaveRoom());

      // Header room code copy
      this.bindClick('btn-copy-header-code', () => this.copyRoomCode());

      // Settings Handlers
      this.bindClick('btn-settings-copy-code', () => this.copyRoomCode());
      this.bindClick('btn-settings-leave-room', () => this.confirmLeaveRoom());
      this.bindClick('btn-clear-local-chat', () => this.confirmClearChat());
      this.bindClick('btn-signout', () => this.confirmSignOut());

      // Modal Triggers
      this.bindClick('btn-close-create-modal', () => this.closeModal('modal-create-room'));
      this.bindClick('btn-close-join-modal', () => this.closeModal('modal-join-room'));
      this.bindClick('btn-modal-copy-code', () => this.copyRoomCode());
      this.bindClick('btn-modal-share-room', () => this.shareRoom());
      this.bindClick('btn-modal-enter-room', () => {
        this.closeModal('modal-create-room');
        this.switchView('watch');
      });

      // Form Submissions
      const authForm = document.getElementById('auth-form');
      if (authForm) {
        authForm.addEventListener('submit', (e) => {
          e.preventDefault();
          this.handleAuthSubmit();
        });
      }

      this.bindClick('btn-create-account', () => this.handleAuthSubmit());
      this.bindClick('btn-demo-login', () => this.quickDemoLogin());
      this.bindClick('btn-forgot-password', () => {
        this.showToast('Password reset link sent to your email (demo).');
      });

      // Join Form Submit
      const joinForm = document.getElementById('join-room-form');
      if (joinForm) {
        joinForm.addEventListener('submit', (e) => {
          e.preventDefault();
          this.handleJoinSubmit();
        });
      }

      // Settings Selects and Switches
      const ctrlSelect = document.getElementById('setting-controller-mode');
      if (ctrlSelect) {
        ctrlSelect.addEventListener('change', (e) => {
          AppState.room.controllerMode = e.target.value;
          updateControllerModeUI();
          sendRoomEvent({
            type: 'CONTROLLER_MODE_CHANGE',
            mode: e.target.value
          });
          this.showToast(`Controller mode: ${e.target.value === 'both' ? 'Both Control' : 'Room Creator Only'}`);
        });
      }

      const darkModeToggle = document.getElementById('setting-dark-mode');
      if (darkModeToggle) {
        // Load saved theme
        const savedTheme = localStorage.getItem('wbu_theme') || 'light';
        const isDark = savedTheme === 'dark';
        document.documentElement.setAttribute('data-theme', savedTheme);
        darkModeToggle.checked = isDark;

        darkModeToggle.addEventListener('change', (e) => {
          const newTheme = e.target.checked ? 'dark' : 'light';
          document.documentElement.setAttribute('data-theme', newTheme);
          localStorage.setItem('wbu_theme', newTheme);
        });
      }

      // Offline toggle simulation
      this.bindClick('btn-toggle-sim-offline', () => NetworkManager.toggleSimulatedOffline());
      
      // Multi-tab sync tester
      this.bindClick('btn-test-multi-tab', () => {
        window.open(window.location.href, '_blank');
      });
    },

    bindClick(id, handler) {
      const el = document.getElementById(id);
      if (el) el.addEventListener('click', handler);
    },

    switchView(viewName) {
      // Hide all views
      document.querySelectorAll('.view-screen').forEach(el => el.classList.add('hidden'));

      const targetViewEl = document.getElementById(`view-${viewName}`);
      if (targetViewEl) {
        targetViewEl.classList.remove('hidden');
        AppState.currentView = viewName;
      }

      // Update Nav Tabs
      document.querySelectorAll('.nav-tab').forEach(tab => {
        const tabView = tab.getAttribute('data-view');
        tab.classList.toggle('active', tabView === viewName);
      });

      // Clear chat badge if entering chat
      if (viewName === 'chat') {
        AppState.chat.unreadCount = 0;
        const badge = document.getElementById('chat-nav-badge');
        if (badge) badge.classList.add('hidden');
        ChatUI.refreshMessages();
      }

      // Scroll to top
      window.scrollTo(0, 0);
    },

    handleAuthSubmit() {
      const email = document.getElementById('auth-email').value;
      const name = document.getElementById('auth-name').value;
      const password = document.getElementById('auth-password').value;

      if (!email || !email.includes('@')) {
        this.showInputError('auth-email', 'Please enter a valid email address.');
        return;
      }
      if (!name || name.trim().length === 0) {
        this.showInputError('auth-name', 'Please enter your name or nickname.');
        return;
      }
      if (!password || password.length < 4) {
        this.showInputError('auth-password', 'Password must be at least 4 characters.');
        return;
      }

      this.clearInputErrors();
      this.loginUser({
        id: 'usr_' + Date.now(),
        name: name.trim(),
        email: email.trim()
      });
    },

    quickDemoLogin() {
      this.loginUser({
        id: 'usr_sathish',
        name: 'Sathish',
        email: 'sathish@example.com'
      });
    },

    loginUser(user) {
      AppState.user = user;
      localStorage.setItem('wbu_user', JSON.stringify(user));

      // Update Profile UI
      const avatarInitial = user.name.charAt(0).toUpperCase();
      const homeUser = document.getElementById('home-user-name');
      const homeAvatar = document.getElementById('user-avatar-initial');
      const setAvatar = document.getElementById('settings-avatar-initial');
      const setName = document.getElementById('settings-user-name');
      const setEmail = document.getElementById('settings-user-email');

      if (homeUser) homeUser.textContent = `Welcome back, ${user.name} ❤️`;
      if (homeAvatar) homeAvatar.textContent = avatarInitial;
      if (setAvatar) setAvatar.textContent = avatarInitial;
      if (setName) setName.textContent = user.name;
      if (setEmail) setEmail.textContent = user.email;

      // Show bottom nav
      document.getElementById('bottom-nav').classList.remove('hidden');

      // Check if previously in room
      const savedRoom = JSON.parse(sessionStorage.getItem('wbu_active_room') || 'null');
      if (savedRoom && savedRoom.code) {
        AppState.room.code = savedRoom.code;
        AppState.room.isCreator = savedRoom.isCreator;
        AppState.room.isConnected = true;
      }

      updateRoomUI();
      this.switchView('home');
      this.showToast(`❤️ Welcome back, ${user.name}!`);
    },

    openCreateRoomModal() {
      const code = createRoom();
      document.getElementById('created-room-code').textContent = code;
      
      const modalStatus = document.getElementById('create-modal-partner-status');
      const statusText = document.getElementById('create-modal-status-text');
      if (modalStatus && statusText) {
        statusText.textContent = 'Waiting for your partner…';
      }

      this.openModal('modal-create-room');
    },

    openJoinRoomModal() {
      const codeInput = document.getElementById('join-room-code-input');
      const errorText = document.getElementById('join-error-text');
      const stateEl = document.getElementById('join-connecting-state');

      if (codeInput) codeInput.value = '';
      if (errorText) errorText.textContent = '';
      if (stateEl) stateEl.classList.add('hidden');

      this.openModal('modal-join-room');
    },

    async handleJoinSubmit() {
      const codeInput = document.getElementById('join-room-code-input');
      const errorText = document.getElementById('join-error-text');
      const stateEl = document.getElementById('join-connecting-state');
      const stateText = document.getElementById('join-state-text');
      const btnSubmit = document.getElementById('btn-submit-join');

      const code = codeInput.value.trim().toUpperCase();
      if (!code) {
        errorText.textContent = "Please enter a room code.";
        return;
      }

      errorText.textContent = '';
      stateEl.classList.remove('hidden');
      stateText.textContent = 'Connecting…';
      btnSubmit.disabled = true;

      try {
        await joinRoom(code);
        stateText.textContent = '❤️ Connected!';
        setTimeout(() => {
          btnSubmit.disabled = false;
          this.closeModal('modal-join-room');
          updateRoomUI();
          this.switchView('watch');
          this.showToast(`❤️ Connected to ${code}`);
        }, 600);
      } catch (err) {
        btnSubmit.disabled = false;
        stateEl.classList.add('hidden');
        errorText.textContent = err.message || "Room not found. Try again.";
      }
    },

    openModal(modalId) {
      const modal = document.getElementById(modalId);
      if (modal) modal.classList.remove('hidden');
    },

    closeModal(modalId) {
      const modal = document.getElementById(modalId);
      if (modal) modal.classList.add('hidden');
    },

    showConfirmModal(title, message, onConfirm) {
      const modal = document.getElementById('modal-confirm');
      const titleEl = document.getElementById('confirm-dialog-title');
      const msgEl = document.getElementById('confirm-dialog-message');
      const okBtn = document.getElementById('btn-confirm-ok');
      const cancelBtn = document.getElementById('btn-confirm-cancel');

      if (!modal) return;
      titleEl.textContent = title;
      msgEl.textContent = message;

      const handleOk = () => {
        modal.classList.add('hidden');
        okBtn.removeEventListener('click', handleOk);
        cancelBtn.removeEventListener('click', handleCancel);
        onConfirm();
      };

      const handleCancel = () => {
        modal.classList.add('hidden');
        okBtn.removeEventListener('click', handleOk);
        cancelBtn.removeEventListener('click', handleCancel);
      };

      okBtn.addEventListener('click', handleOk);
      cancelBtn.addEventListener('click', handleCancel);
      modal.classList.remove('hidden');
    },

    confirmLeaveRoom() {
      this.showConfirmModal(
        'Leave Couple Room',
        'Are you sure you want to disconnect from this room session?',
        () => disconnectFromRoom()
      );
    },

    confirmClearChat() {
      this.showConfirmModal(
        'Clear Chat History',
        'This will erase all local chat messages on this device. Continue?',
        async () => {
          await StorageManager.clearAllChat();
          AppState.chat.messages = [];
          ChatUI.refreshMessages();
          this.showToast('Local chat history cleared.');
        }
      );
    },

    confirmSignOut() {
      this.showConfirmModal(
        'Sign Out',
        'Are you sure you want to sign out?',
        () => {
          if (AppState.room.isConnected) disconnectFromRoom();
          AppState.user = null;
          localStorage.removeItem('wbu_user');
          document.getElementById('bottom-nav').classList.add('hidden');
          this.switchView('auth');
          this.showToast('Signed out successfully.');
        }
      );
    },

    async copyRoomCode() {
      const code = AppState.room.code;
      if (!code) {
        this.showToast('No active room code to copy.');
        return;
      }
      try {
        if (navigator.clipboard) {
          await navigator.clipboard.writeText(code);
          this.showToast(`📋 Room code copied: ${code}`);
        } else {
          prompt('Copy room code:', code);
        }
      } catch (err) {
        prompt('Copy room code:', code);
      }
    },

    async shareRoom() {
      const code = AppState.room.code;
      if (!code) return;

      const shareData = {
        title: 'Web Between Us',
        text: `Join my private couple movie room on Web Between Us! Room Code: ${code}`,
        url: window.location.href
      };

      if (navigator.share) {
        try {
          await navigator.share(shareData);
        } catch (e) {}
      } else {
        this.copyRoomCode();
      }
    },

    showToast(message, type = 'info') {
      const container = document.getElementById('toast-container');
      if (!container) return;

      const toast = document.createElement('div');
      toast.className = `toast ${type}`;
      toast.textContent = message;

      container.appendChild(toast);

      setTimeout(() => {
        toast.classList.add('toast-leave');
        setTimeout(() => toast.remove(), 250);
      }, 3000);
    },

    showInputError(inputId, msg) {
      const errEl = document.getElementById(`${inputId}-error`);
      if (errEl) errEl.textContent = msg;
    },

    clearInputErrors() {
      document.querySelectorAll('.input-error').forEach(el => el.textContent = '');
    }
  };

  /* --------------------------------------------------------------------------
     10. UI STATE SYNCHRONIZERS
     -------------------------------------------------------------------------- */
  function updateRoomUI() {
    const isConnected = !!AppState.room.code;
    const activeCard = document.getElementById('active-room-card');
    const roomTitle = document.getElementById('active-room-title');
    const roomCodeEl = document.getElementById('active-room-code');
    const headerCode = document.getElementById('watch-header-room-code');
    const settingsCode = document.getElementById('settings-room-code-desc');

    if (activeCard) {
      activeCard.classList.toggle('hidden', !isConnected);
    }
    if (isConnected) {
      if (roomTitle) roomTitle.textContent = `❤️ Connected to ${AppState.room.partner.name || 'Partner'}'s Room`;
      if (roomCodeEl) roomCodeEl.textContent = AppState.room.code;
      if (headerCode) headerCode.textContent = AppState.room.code;
      if (settingsCode) settingsCode.textContent = AppState.room.code;
    } else {
      if (headerCode) headerCode.textContent = 'None';
      if (settingsCode) settingsCode.textContent = 'Not in a room';
    }

    updatePartnerPresenceUI(AppState.room.partner.isOnline);
    updateControllerModeUI();
  }

  function updatePartnerPresenceUI(isOnline) {
    const dot = document.getElementById('partner-status-dot');
    const text = document.getElementById('partner-status-text');
    const chatDot = document.getElementById('chat-partner-presence-dot');
    const chatStatus = document.getElementById('chat-partner-status-text');
    const usersLabel = document.getElementById('room-users-label');

    const partnerName = AppState.room.partner.name || 'Partner';

    if (usersLabel) {
      usersLabel.textContent = `You + ${partnerName}`;
    }

    if (dot && text) {
      dot.style.backgroundColor = isOnline ? 'var(--status-online)' : 'var(--status-offline)';
      text.textContent = isOnline ? 'Partner Connected' : 'Waiting for partner…';
    }

    if (chatDot && chatStatus) {
      chatDot.className = `partner-presence-dot ${isOnline ? 'online' : 'offline'}`;
      chatStatus.textContent = isOnline ? 'Online' : 'Offline';
    }
  }

  function updateControllerModeUI() {
    const title = document.getElementById('ctrl-mode-title');
    const desc = document.getElementById('ctrl-mode-desc');
    const select = document.getElementById('setting-controller-mode');

    const isCreator = AppState.room.isCreator;
    const isBoth = AppState.room.controllerMode === 'both';
    const creatorName = AppState.room.partner.name || 'Room Creator';

    if (isBoth) {
      if (title) title.textContent = 'Shared Controls: ON';
      if (desc) desc.textContent = 'Both you and your partner can Play, Pause, and Seek.';
    } else {
      if (isCreator) {
        if (title) title.textContent = '👑 Room Creator Mode (You Control Playback)';
        if (desc) desc.textContent = 'You control Play, Pause, and Seek for both devices.';
      } else {
        if (title) title.textContent = `👀 Watching with ${creatorName}`;
        if (desc) desc.textContent = `Synchronized playback. (${creatorName} controls playback)`;
      }
    }

    if (select) select.value = AppState.room.controllerMode;
  }

  /* --------------------------------------------------------------------------
     11. HELPER UTILITIES
     -------------------------------------------------------------------------- */
  function formatTime(seconds) {
    if (isNaN(seconds) || seconds < 0) return '00:00';
    const hrs = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);

    const pad = (n) => (n < 10 ? '0' + n : n);
    if (hrs > 0) {
      return `${pad(hrs)}:${pad(mins)}:${pad(secs)}`;
    }
    return `${pad(mins)}:${pad(secs)}`;
  }

  function formatShortTime(timestamp) {
    const d = new Date(timestamp);
    let hours = d.getHours();
    let minutes = d.getMinutes();
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12 || 12;
    minutes = minutes < 10 ? '0' + minutes : minutes;
    return `${hours}:${minutes} ${ampm}`;
  }

  function escapeHTML(str) {
    if (!str) return '';
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  /* --------------------------------------------------------------------------
     12. APPLICATION BOOTSTRAP
     -------------------------------------------------------------------------- */
  document.addEventListener('DOMContentLoaded', async () => {
    console.log('[Web Between Us] Starting application...');

    // Initialize core subsystems
    await StorageManager.init();
    RealtimeLayer.init();
    NetworkManager.init();
    VideoController.init();
    ChatUI.init();
    UIManager.init();

    // Check existing user session
    const savedUser = JSON.parse(localStorage.getItem('wbu_user') || 'null');
    if (savedUser && savedUser.email) {
      UIManager.loginUser(savedUser);
    } else {
      UIManager.switchView('auth');
    }

    console.log('[Web Between Us] Ready. “Together, even when we\'re apart.” ❤️');
  });

})();
