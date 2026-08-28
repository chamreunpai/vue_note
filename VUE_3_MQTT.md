To listen to MQTT messages in Vue 3 with TypeScript, use the **`mqtt`** package (v5+) combined with Vue's `onMounted` and `onUnmounted` lifecycle hooks to manage the connection cleanly.

### 1. Installation

Install the MQTT client library:

```bash
npm install mqtt

```

---

### 2. Implementation (`App.vue` or Component)

Here is a fully typed Vue 3 `<script setup>` implementation using the Composition API:

```vue
<template>
  <div class="mqtt-container">
    <h2>MQTT Message Listener</h2>
    
    <div class="status">
      Status: <strong>{{ isConnected ? 'Connected' : 'Disconnected' }}</strong>
    </div>

    <div class="messages">
      <h3>Received Messages:</h3>
      <ul>
        <li v-for="(msg, index) in messages" :key="index">
          <strong>[{{ msg.topic }}]:</strong> {{ msg.payload }}
        </li>
      </ul>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import mqtt, { type MqttClient } from 'mqtt'

// Define types for received messages
interface MqttMessage {
  topic: string
  payload: string
}

// Reactive state
const isConnected = ref<boolean>(false)
const messages = ref<MqttMessage[]>([])
const topicToSubscribe = 'sensors/temperature'

// Retain client instance outside of state to prevent reactivity overhead
let client: MqttClient | null = null

// WebSocket MQTT broker endpoint (Use wss:// for production over HTTPS)
const BROKER_URL = 'wss://broker.emqx.io:8084/mqtt'

const connectMqtt = () => {
  client = mqtt.connect(BROKER_URL, {
    clientId: `vue3_client_${Math.random().toString(16).substring(2, 8)}`,
    clean: true,
    connectTimeout: 4000,
    reconnectPeriod: 1000,
  })

  // Connection successful
  client.on('connect', () => {
    isConnected.value = true
    console.log('Connected to MQTT Broker')

    // Subscribe to topic
    client?.subscribe(topicToSubscribe, (err) => {
      if (err) {
        console.error('Subscription error:', err)
      } else {
        console.log(`Subscribed to topic: ${topicToSubscribe}`)
      }
    })
  })

  // Listen for incoming messages
  client.on('message', (topic: string, message: Buffer) => {
    const payloadStr = message.toString()
    messages.value.unshift({
      topic,
      payload: payloadStr,
    })
  })

  // Handle errors and disconnects
  client.on('error', (err) => {
    console.error('MQTT Connection Error:', err)
  })

  client.on('offline', () => {
    isConnected.value = false
  })
}

// Manage lifecycle hooks
onMounted(() => {
  connectMqtt()
})

onUnmounted(() => {
  if (client) {
    client.end(true) // Force close connection on component destroy
  }
})
</script>

```

---

### Key Points to Keep in Mind

* **Protocol selection:** Browsers cannot establish raw TCP socket connections. You must use WebSocket protocols (`ws://` or `wss://`) on the broker rather than standard MQTT port `1883`.
* **Avoid wrapping the client in `ref()` or `reactive()`:** Vue's reactive proxy will wrap internal methods of the `MqttClient` instance, which can break socket event handlers. Keep `client` as a plain variable (`let client: MqttClient | null = null`).
* **Clean up connections:** Always invoke `client.end()` inside `onUnmounted()` to prevent memory leaks and duplicate connections when components re-render or switch routes.

Here is how to structure the zero-dependency MQTT WebSocket implementation cleanly using a reusable TypeScript file (composable) and then consuming it in a Vue 3 component.

---

### 1. Create the Composable File (`src/composables/useNativeMqtt.ts`)

Create a new file named `useNativeMqtt.ts` to encapsulate all the binary frame building, WebSocket lifecycle management, and reactive states.

```typescript
import { ref, onMounted, onUnmounted, type Ref } from 'vue'

export interface MqttMessage {
  topic: string
  payload: string
}

export interface UseNativeMqttOptions {
  brokerUrl: string
  topic: string
}

export interface UseNativeMqttReturn {
  isConnected: Ref<boolean>
  messages: Ref<MqttMessage[]>
  publish: (topic: string, message: string) => void
  disconnect: () => void
}

export function useNativeMqtt(options: UseNativeMqttOptions): UseNativeMqttReturn {
  const isConnected = ref<boolean>(false)
  const messages = ref<MqttMessage[]>([])
  
  let socket: WebSocket | null = null

  // Helper: Encode String into [Length MSB, Length LSB, ...UTF8Bytes]
  const encodeString = (str: string): number[] => {
    const encoder = new TextEncoder()
    const bytes = encoder.encode(str)
    return [(bytes.length >> 8) & 0xff, bytes.length & 0xff, ...Array.from(bytes)]
  }

  // 1. MQTT CONNECT Packet (0x10)
  const createConnectPacket = (clientId: string): Uint8Array => {
    const protocolName = encodeString('MQTT')
    const protocolLevel = [0x04] // MQTT 3.1.1
    const connectFlags = [0x02] // Clean session
    const keepAlive = [0x00, 0x3c] // 60s keepalive
    const clientPayload = encodeString(clientId)

    const payload = [
      ...protocolName,
      ...protocolLevel,
      ...connectFlags,
      ...keepAlive,
      ...clientPayload,
    ]

    return new Uint8Array([0x10, payload.length, ...payload])
  }

  // 2. MQTT SUBSCRIBE Packet (0x82)
  const createSubscribePacket = (topic: string, packetId = 1): Uint8Array => {
    const packetIdBytes = [(packetId >> 8) & 0xff, packetId & 0xff]
    const topicBytes = encodeString(topic)
    const qosByte = [0x00] // QoS 0

    const payload = [...packetIdBytes, ...topicBytes, ...qosByte]

    return new Uint8Array([0x82, payload.length, ...payload])
  }

  // 3. MQTT PUBLISH Packet (0x30)
  const createPublishPacket = (topic: string, message: string): Uint8Array => {
    const topicBytes = encodeString(topic)
    const encoder = new TextEncoder()
    const payloadBytes = Array.from(encoder.encode(message))

    const variableHeaderAndPayload = [...topicBytes, ...payloadBytes]

    return new Uint8Array([0x30, variableHeaderAndPayload.length, ...variableHeaderAndPayload])
  }

  // Connect to the Broker
  const connect = () => {
    socket = new WebSocket(options.brokerUrl, 'mqtt')
    socket.binaryType = 'arraybuffer'

    socket.onopen = () => {
      const clientId = `vue_ts_${Math.random().toString(16).substring(2, 8)}`
      socket?.send(createConnectPacket(clientId))
    }

    socket.onmessage = (event: MessageEvent) => {
      const data = new Uint8Array(event.data as ArrayBuffer)
      const controlPacketType = data[0] >> 4

      // Type 2: CONNACK
      if (controlPacketType === 2) {
        if (data[3] === 0x00) {
          isConnected.value = true
          // Automatically subscribe once connected
          socket?.send(createSubscribePacket(options.topic))
        }
      }

      // Type 3: PUBLISH (Incoming Message)
      if (controlPacketType === 3) {
        const topicLen = (data[2] << 8) | data[3]
        const topicBytes = data.subarray(4, 4 + topicLen)
        const payloadBytes = data.subarray(4 + topicLen)

        const decoder = new TextDecoder()
        const topic = decoder.decode(topicBytes)
        const payload = decoder.decode(payloadBytes)

        messages.value.unshift({ topic, payload })
      }
    }

    socket.onerror = (err) => {
      console.error('MQTT WebSocket Error:', err)
    }

    socket.onclose = () => {
      isConnected.value = false
    }
  }

  // Expose publish function
  const publish = (topic: string, message: string) => {
    if (socket && isConnected.value) {
      socket.send(createPublishPacket(topic, message))
    } else {
      console.warn('Cannot publish: MQTT is disconnected.')
    }
  }

  // Clean close
  const disconnect = () => {
    if (socket) {
      socket.close()
      socket = null
    }
  }

  onMounted(() => {
    connect()
  })

  onUnmounted(() => {
    disconnect()
  })

  return {
    isConnected,
    messages,
    publish,
    disconnect,
  }
}

```

---

### 2. Consume in a Vue 3 Component (`src/components/MqttDemo.vue`)

Import and invoke the composable inside your component's `<script setup>` tag:

```vue
<template>
  <div class="mqtt-demo">
    <h2>Native MQTT Composable Demo</h2>

    <!-- Connection Status -->
    <div class="status-badge" :class="{ active: isConnected }">
      Status: {{ isConnected ? 'CONNECTED' : 'DISCONNECTED' }}
    </div>

    <!-- Message Sender -->
    <div class="send-box">
      <input 
        v-model="inputMessage" 
        placeholder="Type a message to send..." 
        :disabled="!isConnected"
        @keyup.enter="sendMessage"
      />
      <button :disabled="!isConnected" @click="sendMessage">Publish</button>
    </div>

    <!-- Message Log -->
    <div class="message-log">
      <h3>Received Messages</h3>
      <p v-if="messages.length === 0">Waiting for incoming messages...</p>
      <ul>
        <li v-for="(msg, index) in messages" :key="index">
          <span class="topic">[{{ msg.topic }}]</span>
          <span class="payload">{{ msg.payload }}</span>
        </li>
      </ul>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useNativeMqtt } from '../composables/useNativeMqtt'

const inputMessage = ref('')

// Initialize the composable
const { isConnected, messages, publish } = useNativeMqtt({
  brokerUrl: 'wss://broker.emqx.io:8084/mqtt',
  topic: 'sensors/temperature',
})

const sendMessage = () => {
  if (!inputMessage.value.trim()) return
  
  // Publish message to the same topic
  publish('sensors/temperature', inputMessage.value)
  inputMessage.value = ''
}
</script>

<style scoped>
.status-badge {
  display: inline-block;
  padding: 4px 12px;
  border-radius: 4px;
  background-color: #f44336;
  color: white;
  font-weight: bold;
}
.status-badge.active {
  background-color: #4caf50;
}
.send-box {
  margin: 16px 0;
  display: flex;
  gap: 8px;
}
.message-log {
  border: 1px solid #ccc;
  padding: 12px;
  border-radius: 6px;
}
.topic {
  font-weight: bold;
  color: #0066cc;
  margin-right: 8px;
}
</style>

```

Your current code does **not match the MQTT configuration in the image**.

From the image:

* Host: `tcp://uat-api.loukdo.com:31883`
* Username: `social-mqtt`
* Password: configured
* QoS: `1`

The biggest problem is:

> **A browser cannot connect directly to `tcp://` MQTT using `WebSocket`.**

`new WebSocket()` requires an MQTT WebSocket endpoint such as:

```text
ws://host:port
```

or:

```text
wss://host:port
```

Also, your current MQTT `CONNECT` packet does **not include username and password**, and your subscribe QoS is hardcoded to `0`.

Here is a corrected native MQTT composable with:

* Username/password authentication
* QoS `0`, `1`, or `2`
* MQTT 3.1.1 packets
* Proper MQTT Remaining Length encoding
* Incoming QoS 1 support with `PUBACK`
* `PINGREQ` keepalive
* `DISCONNECT`
* Unique packet IDs
* Safer WebSocket handling

## `useNativeMqtt.ts`

```ts
import {
  ref,
  onMounted,
  onUnmounted,
  type Ref,
} from "vue";

export interface MqttMessage {
  topic: string;
  payload: string;
  qos: 0 | 1 | 2;
}

export interface UseNativeMqttOptions {
  /**
   * IMPORTANT:
   * Browser WebSocket cannot use tcp://
   *
   * Example:
   * ws://uat-api.loukdo.com:PORT
   * wss://uat-api.loukdo.com:PORT
   */
  brokerUrl: string;

  username?: string;
  password?: string;

  topic: string;

  qos?: 0 | 1 | 2;

  clientId?: string;

  keepAlive?: number;

  autoConnect?: boolean;
}

export interface UseNativeMqttReturn {
  isConnected: Ref<boolean>;

  messages: Ref<MqttMessage[]>;

  connect: () => void;

  publish: (
    topic: string,
    message: string,
    qos?: 0 | 1 | 2,
  ) => void;

  disconnect: () => void;
}

export function useNativeMqtt(
  options: UseNativeMqttOptions,
): UseNativeMqttReturn {
  const isConnected = ref(false);

  const messages = ref<MqttMessage[]>([]);

  let socket: WebSocket | null = null;

  let pingTimer: ReturnType<typeof setInterval> | null = null;

  let packetId = 1;

  const encoder = new TextEncoder();

  const decoder = new TextDecoder();

  /**
   * Generate MQTT packet ID.
   * Valid range: 1 - 65535
   */
  const getNextPacketId = (): number => {
    packetId++;

    if (packetId > 65535) {
      packetId = 1;
    }

    return packetId;
  };

  /**
   * Encode MQTT string:
   *
   * [length MSB][length LSB][UTF8 bytes]
   */
  const encodeString = (
    value: string,
  ): number[] => {
    const bytes = encoder.encode(value);

    return [
      (bytes.length >> 8) & 0xff,
      bytes.length & 0xff,
      ...Array.from(bytes),
    ];
  };

  /**
   * MQTT Remaining Length uses variable byte encoding.
   */
  const encodeRemainingLength = (
    length: number,
  ): number[] => {
    const bytes: number[] = [];

    do {
      let digit = length % 128;

      length = Math.floor(length / 128);

      if (length > 0) {
        digit |= 0x80;
      }

      bytes.push(digit);
    } while (length > 0);

    return bytes;
  };

  /**
   * Decode MQTT Remaining Length.
   */
  const decodeRemainingLength = (
    data: Uint8Array,
    startIndex = 1,
  ): {
    value: number;
    bytesUsed: number;
  } => {
    let multiplier = 1;

    let value = 0;

    let bytesUsed = 0;

    let index = startIndex;

    let encodedByte: number;

    do {
      encodedByte = data[index];

      value += (encodedByte & 127) * multiplier;

      multiplier *= 128;

      index++;

      bytesUsed++;
    } while (
      (encodedByte & 128) !== 0 &&
      index < data.length
    );

    return {
      value,
      bytesUsed,
    };
  };

  /**
   * MQTT CONNECT packet.
   *
   * MQTT 3.1.1
   */
  const createConnectPacket = (
    clientId: string,
  ): Uint8Array => {
    const username =
      options.username?.trim();

    const password =
      options.password;

    let connectFlags = 0;

    /**
     * Clean Session
     */
    connectFlags |= 0x02;

    /**
     * Password Flag
     */
    if (password) {
      connectFlags |= 0x40;
    }

    /**
     * Username Flag
     */
    if (username) {
      connectFlags |= 0x80;
    }

    const keepAlive =
      options.keepAlive ?? 60;

    const variableHeader = [
      ...encodeString("MQTT"),

      /**
       * Protocol Level
       * MQTT 3.1.1 = 4
       */
      0x04,

      connectFlags,

      (keepAlive >> 8) & 0xff,
      keepAlive & 0xff,
    ];

    const payload = [
      ...encodeString(clientId),
    ];

    if (username) {
      payload.push(
        ...encodeString(username),
      );
    }

    if (password) {
      payload.push(
        ...encodeString(password),
      );
    }

    const remainingLength =
      variableHeader.length +
      payload.length;

    return new Uint8Array([
      0x10,

      ...encodeRemainingLength(
        remainingLength,
      ),

      ...variableHeader,

      ...payload,
    ]);
  };

  /**
   * MQTT SUBSCRIBE packet.
   *
   * QoS must be requested with packet type flags:
   * 0x82
   */
  const createSubscribePacket = (
    topic: string,
    qos: 0 | 1 | 2,
    id: number,
  ): Uint8Array => {
    const packetIdBytes = [
      (id >> 8) & 0xff,
      id & 0xff,
    ];

    const payload = [
      ...packetIdBytes,

      ...encodeString(topic),

      qos,
    ];

    return new Uint8Array([
      0x82,

      ...encodeRemainingLength(
        payload.length,
      ),

      ...payload,
    ]);
  };

  /**
   * MQTT PUBLISH packet.
   */
  const createPublishPacket = (
    topic: string,
    message: string,
    qos: 0 | 1 | 2 = 0,
    id?: number,
  ): Uint8Array => {
    let header = 0x30;

    /**
     * Set QoS bits.
     */
    header |= qos << 1;

    const variableHeader = [
      ...encodeString(topic),
    ];

    /**
     * QoS 1 and QoS 2 require Packet Identifier.
     */
    if (qos > 0 && id) {
      variableHeader.push(
        (id >> 8) & 0xff,
        id & 0xff,
      );
    }

    const payload = Array.from(
      encoder.encode(message),
    );

    const body = [
      ...variableHeader,
      ...payload,
    ];

    return new Uint8Array([
      header,

      ...encodeRemainingLength(
        body.length,
      ),

      ...body,
    ]);
  };

  /**
   * PUBACK for incoming QoS 1 message.
   */
  const createPubAckPacket = (
    id: number,
  ): Uint8Array => {
    return new Uint8Array([
      0x40,
      0x02,

      (id >> 8) & 0xff,
      id & 0xff,
    ]);
  };

  /**
   * PINGREQ
   */
  const createPingPacket =
    (): Uint8Array => {
      return new Uint8Array([
        0xc0,
        0x00,
      ]);
    };

  /**
   * DISCONNECT
   */
  const createDisconnectPacket =
    (): Uint8Array => {
      return new Uint8Array([
        0xe0,
        0x00,
      ]);
    };

  /**
   * Stop keepalive timer.
   */
  const stopPing = (): void => {
    if (pingTimer) {
      clearInterval(pingTimer);

      pingTimer = null;
    }
  };

  /**
   * Start MQTT keepalive.
   */
  const startPing = (): void => {
    stopPing();

    const keepAlive =
      options.keepAlive ?? 60;

    /**
     * Do not create timer if disabled.
     */
    if (keepAlive <= 0) {
      return;
    }

    pingTimer = setInterval(() => {
      if (
        socket &&
        socket.readyState === WebSocket.OPEN &&
        isConnected.value
      ) {
        socket.send(
          createPingPacket(),
        );
      }
    }, keepAlive * 1000);
  };

  /**
   * Handle incoming MQTT PUBLISH.
   */
  const handlePublish = (
    data: Uint8Array,
    firstByte: number,
    bodyStart: number,
    remainingLength: number,
  ): void => {
    const qos =
      ((firstByte >> 1) & 0x03) as
        | 0
        | 1
        | 2;

    const topicLength =
      (data[bodyStart] << 8) |
      data[bodyStart + 1];

    const topicStart =
      bodyStart + 2;

    const topicEnd =
      topicStart + topicLength;

    const topic = decoder.decode(
      data.subarray(
        topicStart,
        topicEnd,
      ),
    );

    let payloadStart = topicEnd;

    let incomingPacketId: number | null =
      null;

    /**
     * QoS 1 / 2 contain Packet Identifier.
     */
    if (qos > 0) {
      incomingPacketId =
        (data[payloadStart] << 8) |
        data[payloadStart + 1];

      payloadStart += 2;
    }

    const payloadEnd =
      bodyStart + remainingLength;

    const payload = decoder.decode(
      data.subarray(
        payloadStart,
        payloadEnd,
      ),
    );

    messages.value.unshift({
      topic,
      payload,
      qos,
    });

    /**
     * Acknowledge QoS 1 message.
     */
    if (
      qos === 1 &&
      incomingPacketId !== null
    ) {
      socket?.send(
        createPubAckPacket(
          incomingPacketId,
        ),
      );
    }

    /**
     * QoS 2 requires PUBREC/PUBREL/PUBCOMP.
     * This example does not implement full QoS 2 flow.
     */
  };

  /**
   * Process MQTT packet.
   */
  const handleMessage = (
    event: MessageEvent,
  ): void => {
    const data = new Uint8Array(
      event.data as ArrayBuffer,
    );

    if (data.length < 2) {
      return;
    }

    const firstByte = data[0];

    const controlPacketType =
      firstByte >> 4;

    const {
      value: remainingLength,
      bytesUsed,
    } = decodeRemainingLength(
      data,
    );

    const bodyStart =
      1 + bytesUsed;

    /**
     * CONNACK
     */
    if (controlPacketType === 2) {
      /**
       * CONNACK body:
       *
       * byte 1 = acknowledge flags
       * byte 2 = return code
       */
      const returnCode =
        data[bodyStart + 1];

      if (returnCode === 0x00) {
        isConnected.value = true;

        console.log(
          "MQTT connected successfully",
        );

        const qos =
          options.qos ?? 1;

        socket?.send(
          createSubscribePacket(
            options.topic,
            qos,
            getNextPacketId(),
          ),
        );

        startPing();
      } else {
        console.error(
          "MQTT connection refused:",
          returnCode,
        );

        disconnect();
      }

      return;
    }

    /**
     * PUBLISH
     */
    if (controlPacketType === 3) {
      handlePublish(
        data,
        firstByte,
        bodyStart,
        remainingLength,
      );

      return;
    }

    /**
     * SUBACK
     */
    if (controlPacketType === 9) {
      console.log(
        "MQTT subscription successful",
      );

      return;
    }

    /**
     * PINGRESP
     */
    if (controlPacketType === 13) {
      return;
    }
  };

  /**
   * Connect to MQTT WebSocket broker.
   */
  const connect = (): void => {
    /**
     * Prevent multiple connections.
     */
    if (
      socket &&
      (
        socket.readyState === WebSocket.OPEN ||
        socket.readyState === WebSocket.CONNECTING
      )
    ) {
      return;
    }

    /**
     * Browser cannot use tcp://.
     */
    if (
      options.brokerUrl.startsWith(
        "tcp://",
      )
    ) {
      console.error(
        [
          "Invalid MQTT URL for browser.",
          "",
          "WebSocket cannot connect to tcp://",
          `Current URL: ${options.brokerUrl}`,
          "",
          "You need an MQTT WebSocket endpoint:",
          "ws://host:port",
          "or",
          "wss://host:port",
        ].join("\n"),
      );

      return;
    }

    try {
      socket = new WebSocket(
        options.brokerUrl,
        "mqtt",
      );

      socket.binaryType = "arraybuffer";

      socket.onopen = () => {
        const clientId =
          options.clientId ??
          `vue_mqtt_${crypto.randomUUID()}`;

        socket?.send(
          createConnectPacket(
            clientId,
          ),
        );
      };

      socket.onmessage =
        handleMessage;

      socket.onerror = (error) => {
        console.error(
          "MQTT WebSocket Error:",
          error,
        );
      };

      socket.onclose = () => {
        console.log(
          "MQTT disconnected",
        );

        isConnected.value = false;

        stopPing();

        socket = null;
      };
    } catch (error) {
      console.error(
        "Cannot connect to MQTT:",
        error,
      );

      isConnected.value = false;
    }
  };

  /**
   * Publish message.
   */
  const publish = (
    topic: string,
    message: string,
    qos: 0 | 1 | 2 = 0,
  ): void => {
    if (
      !socket ||
      socket.readyState !== WebSocket.OPEN ||
      !isConnected.value
    ) {
      console.warn(
        "Cannot publish: MQTT is disconnected.",
      );

      return;
    }

    /**
     * QoS 2 is not fully implemented in this
     * lightweight native client.
     */
    if (qos === 2) {
      console.warn(
        "QoS 2 publish is not fully supported.",
      );

      return;
    }

    const id =
      qos > 0
        ? getNextPacketId()
        : undefined;

    socket.send(
      createPublishPacket(
        topic,
        message,
        qos,
        id,
      ),
    );
  };

  /**
   * Disconnect.
   */
  const disconnect = (): void => {
    stopPing();

    isConnected.value = false;

    if (!socket) {
      return;
    }

    if (
      socket.readyState === WebSocket.OPEN
    ) {
      try {
        socket.send(
          createDisconnectPacket(),
        );
      } catch (error) {
        console.warn(
          "Failed to send MQTT DISCONNECT:",
          error,
        );
      }
    }

    socket.close();

    socket = null;
  };

  onMounted(() => {
    if (
      options.autoConnect !== false
    ) {
      connect();
    }
  });

  onUnmounted(() => {
    disconnect();
  });

  return {
    isConnected,
    messages,
    connect,
    publish,
    disconnect,
  };
}
```

## How to use it

Your configuration from the image would logically be:

```ts
const {
  isConnected,
  messages,
  publish,
  disconnect,
} = useNativeMqtt({
  brokerUrl: "ws://YOUR_MQTT_WEBSOCKET_HOST:PORT",

  username: "social-mqtt",

  password: "YOUR_PASSWORD",

  topic: "YOUR_TOPIC",

  qos: 1,

  keepAlive: 60,
});
```

### Important

Do **not** use this in the browser:

```ts
brokerUrl: "tcp://uat-api.loukdo.com:31883"
```

This will not work:

```ts
new WebSocket("tcp://uat-api.loukdo.com:31883")
```

You need your MQTT server to expose a WebSocket listener, for example:

```text
ws://uat-api.loukdo.com:9001
```

or:

```text
wss://uat-api.loukdo.com/mqtt
```

The exact WebSocket URL depends on how your MQTT broker is configured.

Also, the screenshot contains a visible MQTT password. Since it has been exposed in the conversation, you should rotate/change that credential if it is a real active password.
