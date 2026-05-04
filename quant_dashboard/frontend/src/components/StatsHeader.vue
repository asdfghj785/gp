<template>
  <header class="stats-header">
    <div class="header-brand">
      <span class="logo">Q</span>
      <div>
        <strong>V4.0 Theme Alpha</strong>
        <small>{{ title }}</small>
      </div>
    </div>

    <section class="stat-strip" aria-label="顶部状态条">
      <article>
        <span>今日锁定</span>
        <strong>{{ lockedCount }}</strong>
      </article>
      <article>
        <span>最近同步</span>
        <strong>{{ latestSyncText }}</strong>
      </article>
      <article>
        <span>后端健康</span>
        <strong :class="health?.ok ? 'ok' : 'bad'">{{ health?.ok ? 'Online' : 'Offline' }}</strong>
      </article>
    </section>

    <section class="header-actions" aria-label="顶部快捷操作">
      <slot name="actions"></slot>
    </section>

    <section class="lights" aria-label="服务指示灯">
      <span :class="['light', syncOk ? 'green' : 'yellow']" title="数据同步状态"></span>
      <span :class="['light', health?.pushplus?.ok ? 'green' : 'red']" title="推送服务状态"></span>
      <span :class="['light', modelOk ? 'green' : 'yellow']" title="模型风控状态"></span>
    </section>
  </header>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  title: { type: String, default: 'Dashboard' },
  lockedCount: { type: Number, default: 0 },
  latestSync: { type: Object, default: null },
  health: { type: Object, default: () => ({}) },
  modelStatus: { type: String, default: '' },
})

const latestSyncText = computed(() => {
  if (!props.latestSync) return '暂无'
  if (props.latestSync.status !== 'success') return '失败'
  return props.latestSync.sync_date || props.latestSync.finished_at || '成功'
})

const syncOk = computed(() => props.latestSync?.status === 'success')
const modelOk = computed(() => Boolean(props.modelStatus && props.modelStatus !== 'no_cache'))
</script>

<style scoped>
.stats-header {
  height: 76px;
  position: sticky;
  top: 0;
  z-index: 10;
  display: grid;
  grid-template-columns: minmax(230px, 0.72fr) minmax(360px, 0.82fr) auto auto;
  gap: 14px;
  align-items: center;
  padding: 12px 18px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
  background: rgba(15, 17, 21, 0.96);
  backdrop-filter: blur(10px);
}

.header-brand {
  display: flex;
  align-items: center;
  gap: 12px;
  min-width: 0;
}

.logo {
  width: 42px;
  height: 42px;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: linear-gradient(135deg, #f5222d, #722ed1);
  color: #fff;
  font-weight: 900;
}

.header-brand strong,
.header-brand small {
  display: block;
}

.header-brand strong {
  color: #f2f6ff;
  font-size: 1rem;
}

.header-brand small,
.stat-strip span {
  color: #6f7d95;
  font-size: 0.76rem;
}

.stat-strip {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
}

.stat-strip article {
  border: 1px solid var(--terminal-border);
  border-radius: 10px;
  padding: 8px 10px;
  background: var(--terminal-card);
  min-width: 0;
}

.stat-strip span,
.stat-strip strong {
  display: block;
}

.stat-strip strong {
  margin-top: 3px;
  color: #f2f6ff;
  font-size: 0.92rem;
  overflow-wrap: anywhere;
}

.header-actions {
  min-width: 0;
}

.header-actions:empty {
  display: none;
}

.lights {
  display: flex;
  align-items: center;
  gap: 10px;
}

.light {
  width: 13px;
  height: 13px;
  border-radius: 999px;
  box-shadow: 0 0 14px currentColor;
}

.green {
  color: var(--quant-fall);
  background: var(--quant-fall);
}

.yellow {
  color: #f5c542;
  background: #f5c542;
}

.red {
  color: var(--quant-rise);
  background: var(--quant-rise);
}

.ok {
  color: var(--quant-fall) !important;
}

.bad {
  color: var(--quant-fall) !important;
}

@media (max-width: 980px) {
  .stats-header {
    position: static;
    height: auto;
    grid-template-columns: 1fr;
  }

  .lights {
    justify-content: flex-start;
  }
}

@media (max-width: 620px) {
  .stat-strip {
    grid-template-columns: 1fr;
  }
}
</style>
