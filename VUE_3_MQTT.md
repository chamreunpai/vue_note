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
