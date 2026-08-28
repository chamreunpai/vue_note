<script setup>
import { ref, onMounted, onUnmounted, watch } from "vue";
import axios from "axios";
import mqtt from "mqtt";

const props = defineProps({
  chatId: {
    type: String,
    default: "1191490956",
  },

  userToken: {
    type: String,
    default: "b0aa63a1-264b-48e1-bc04-4cd81fdd25fa",
  },

  authToken: {
    type: String,
    default:
      "eyJhbGciOiJIUzM4NCJ9.eyJhdXRob3JpdGllcyI6WyJVc2VyIENyZWF0ZXMiXSwic3ViIjoie1wiaWRcIjoyMixcInV1aWRcIjpcIjMyZTNlMTA3LTA1NzgtNGY0NS05OWZkLTA2YzA1YjIwMGYyNlwiLFwidXNlcm5hbWVcIjpudWxsLFwidXNlclwiOntcImlkXCI6MjIsXCJ1c2VybmFtZVwiOm51bGwsXCJmdWxsTmFtZUVOXCI6XCJDb2RpIEpzb25cIixcInBob25lXCI6XCIwMTU3MTI3NTdcIixcImVtYWlsXCI6XCJjb2RpanNvbkBnbWFpbC5jb21cIixcInVzZXJTdGF0dXNcIjoxLFwiYWRtaW5cIjpmYWxzZSxcImltYWdlXCI6XCIzYWI0NDRmMC0xYjE0LTQwNzQtOTI3Ni00YzhkZDQ5ODhlMDYuanBnXCIsXCJzdGF0dXNcIjp0cnVlLFwidXNlclR5cGVcIjoyLFwic3RvcmVJZFwiOjI2LFwic3RvcmVPd25lclwiOnRydWUsXCJyb2xlc1wiOm51bGx9LFwiYWRtaW5cIjpmYWxzZX0iLCJpYXQiOjE3ODc4MjgxMDIsImV4cCI6NTI3MTQ3NjEwMn0.U6eqT4fm3shCM1oCZzDB8z5tKCZm8MlgQiIHmjW_s8bw7Pp1Kmqj3S-9ejXZtukg",
  },
});

const messages = ref([]);
const isConnected = ref(false);
const client = ref(null);
const currentTopic = ref("");

const API_BASE_URL = "https://iot.idealinkconsulting.com/sc-rest/api/v1";

const MQTT_URL = "ws://uat-api.loukdo.com:38010/mqtt";

/**
 * GET:
 *
 * https://iot.idealinkconsulting.com/sc-rest/api/v1/channel/telegram/chats/{chatId}/messages
 *
 * ?userToken={userToken}
 * &limit=30
 */
const fetchMessagesApi = async () => {
  try {
    const response = await axios.get(
      `${API_BASE_URL}/channel/telegram/chats/${props.chatId}/messages`,
      {
        params: {
          userToken: props.userToken,
          limit: 30,
        },

        headers: {
          Authorization: `Bearer ${props.authToken}`,
          Accept: "application/json, text/plain, */*",
        },
      },
    );

    console.log("Telegram API response:", response.data);

    /**
     * API response:
     *
     * {
     *   results: {
     *     messages: [],
     *     hasMore: false,
     *     nextFromMessageId: null
     *   },
     *   response: {
     *     code: 200,
     *     message: "Success"
     *   }
     * }
     */

    messages.value = response.data?.results?.messages ?? [];

    console.log("Messages:", messages.value);
  } catch (error) {
    console.error("Failed to fetch Telegram messages:", error);

    if (error.response) {
      console.error("Status:", error.response.status);

      console.error("Response:", error.response.data);
    }

    messages.value = [];
  }
};

/**
 * Build MQTT topic
 */
const getTopic = () => {
  return `telegram/${props.userToken}${props.chatId}/chats`;
};

/**
 * Subscribe to Telegram chat
 */
const subscribeChannel = () => {
  if (!client.value?.connected) {
    return;
  }

  const topic = getTopic();

  /**
   * Unsubscribe old topic
   */
  if (currentTopic.value && currentTopic.value !== topic) {
    client.value.unsubscribe(currentTopic.value);
  }

  currentTopic.value = topic;

  client.value.subscribe(
    topic,
    {
      qos: 1,
    },
    (error) => {
      if (error) {
        console.error("MQTT subscribe error:", error);

        return;
      }

      console.log("MQTT subscribed:", topic);
    },
  );
};

/**
 * Connect MQTT
 */
const initMqtt = () => {
  if (client.value) {
    client.value.end(true);
    client.value = null;
  }

  client.value = mqtt.connect(MQTT_URL, {
    username: "social-mqtt",
    password: "soci@l_mqTT",

    clientId: `vue3_${Math.random().toString(16).substring(2, 10)}`,

    clean: true,

    reconnectPeriod: 3000,

    connectTimeout: 10000,
  });

  /**
   * Connected
   */
  client.value.on("connect", () => {
    console.log("MQTT connected");

    isConnected.value = true;

    subscribeChannel();
  });

  /**
   * Receive realtime messages
   */
  client.value.on("message", (topic, payload) => {
    const payloadText = payload.toString();

    console.log("MQTT message:", topic, payloadText);

    try {
      const message = JSON.parse(payloadText);

      /**
       * Prevent duplicate message
       */
      const messageId = message?.messageId;

      if (
        messageId &&
        messages.value.some((item) => item.messageId === messageId)
      ) {
        return;
      }

      messages.value.push(message);
    } catch {
      messages.value.push({
        messageId: `mqtt-${Date.now()}`,
        chatId: props.chatId,
        content: {
          type: "text",
          text: payloadText,
        },
      });
    }
  });

  /**
   * Error
   */
  client.value.on("error", (error) => {
    console.error("MQTT error:", error);

    isConnected.value = false;
  });

  /**
   * Disconnected
   */
  client.value.on("close", () => {
    console.log("MQTT disconnected");

    isConnected.value = false;
  });

  /**
   * Reconnecting
   */
  client.value.on("reconnect", () => {
    console.log("MQTT reconnecting...");

    isConnected.value = false;
  });
};

/**
 * Convert API relative media URL
 * to absolute URL.
 */
const getMediaUrl = (url) => {
  if (!url) {
    return "";
  }

  if (url.startsWith("http://") || url.startsWith("https://")) {
    return url;
  }

  return `${API_BASE_URL}/channel/telegram${url}`;
};

/**
 * Initial load
 */
onMounted(async () => {
  await fetchMessagesApi();

  initMqtt();
});

/**
 * Chat changed
 */
watch(
  () => [props.chatId, props.userToken],
  async () => {
    messages.value = [];

    await fetchMessagesApi();

    subscribeChannel();
  },
);

/**
 * Cleanup
 */
onUnmounted(() => {
  if (!client.value) {
    return;
  }

  if (currentTopic.value) {
    client.value.unsubscribe(currentTopic.value);
  }

  client.value.end(true);

  client.value = null;

  isConnected.value = false;
});
</script>

<template>
  <div class="chat-wrapper">
    <!-- Status -->
    <div class="status-bar">
      <div>
        MQTT:

        <strong>
          {{ isConnected ? "Connected" : "Connecting..." }}
        </strong>
      </div>

      <div>
        Topic:

        <code>
          {{ currentTopic }}
        </code>
      </div>

      <div>
        Messages:

        <strong>
          {{ messages.length }}
        </strong>
      </div>
    </div>

    <!-- Messages -->
    <div class="messages-list">
      <div
        v-for="(message, index) in messages"
        :key="message.messageId || index"
        class="message-card"
      >
        <!-- TEXT -->
        <template v-if="message.content?.type === 'text'">
          <div class="message-text">
            {{ message.content.text }}
          </div>
        </template>

        <!-- ANIMATED EMOJI -->
        <template v-else-if="message.content?.type === 'animated_emoji'">
          <div class="emoji">
            {{ message.content.emoji }}
          </div>
        </template>

        <!-- STICKER -->
        <template v-else-if="message.content?.type === 'sticker'">
          <img
            v-if="message.content.thumbnailUrl"
            :src="getMediaUrl(message.content.thumbnailUrl)"
            class="sticker"
            alt="Telegram sticker"
          />

          <div class="sticker-info">
            <span>
              {{ message.content.emoji }}
            </span>

            <span>
              {{ message.content.stickerFormat }}
            </span>
          </div>
        </template>

        <!-- VIDEO -->
        <template v-else-if="message.content?.type === 'video'">
          <video
            controls
            class="video"
            :poster="getMediaUrl(message.content.thumbnailUrl)"
          >
            <source
              :src="getMediaUrl(message.content.downloadUrl)"
              :type="message.content.mimeType"
            />
          </video>

          <div class="caption">
            {{ message.content.caption }}
          </div>
        </template>

        <!-- OTHER -->
        <template v-else>
          <pre>{{ JSON.stringify(message, null, 2) }}</pre>
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.chat-wrapper {
  width: 100%;
  padding: 16px;
}

.status-bar {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 12px;
  margin-bottom: 16px;
  background: #f5f5f5;
  border-radius: 8px;
}

.messages-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.message-card {
  padding: 12px;
  border: 1px solid #ddd;
  border-radius: 8px;
  background: #fafafa;
}

.message-text {
  white-space: pre-wrap;
  word-break: break-word;
}

.emoji {
  font-size: 48px;
}

.sticker {
  display: block;
  width: 180px;
  height: 180px;
  object-fit: contain;
}

.sticker-info {
  display: flex;
  gap: 8px;
  margin-top: 8px;
}

.video {
  display: block;
  max-width: 320px;
  max-height: 500px;
  border-radius: 8px;
}

.caption {
  margin-top: 8px;
}

pre {
  margin: 0;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
}
</style>
