<template>
  <aside class="sidebar">
    <nav class="nav-menu" aria-label="功能导航">
      <button
        v-for="item in navItems"
        :key="item.id"
        type="button"
        :class="{ active: active === item.id }"
        @click="$emit('update:active', item.id)"
      >
        <span class="nav-mark">{{ item.mark }}</span>
        <span>
          <strong>{{ item.label }}</strong>
          <small>{{ item.hint }}</small>
        </span>
      </button>
    </nav>

    <section class="service-card">
      <span>PushPlus 服务</span>
      <strong :class="health?.pushplus?.ok ? 'ok' : 'bad'">
        {{ health?.pushplus?.ok ? 'Ready' : 'Critical' }}
      </strong>
      <small>{{ health?.pushplus?.reason || '等待健康检查' }}</small>
    </section>
  </aside>
</template>

<script setup>
defineProps({
  active: { type: String, required: true },
  health: { type: Object, default: () => ({}) },
})

defineEmits(['update:active'])

const navItems = [
  { id: 'dashboard', mark: 'D', label: 'Dashboard', hint: '总览与今日信号' },
  { id: 'ledger', mark: 'S', label: 'Shadow Test', hint: '分月影子账本' },
  { id: 'minute', mark: 'M', label: '单票行情库', hint: '日K、5m 与原始表格' },
  { id: 'validation', mark: 'V', label: 'Validation', hint: '同步与校验报告' },
]
</script>

<style scoped>
.sidebar {
  height: calc(100vh - 76px);
  position: sticky;
  top: 76px;
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 14px;
  border-right: 1px solid rgba(255, 255, 255, 0.08);
  background: var(--terminal-bg);
}

.nav-menu {
  display: grid;
  gap: 8px;
}

.nav-menu button {
  display: grid;
  grid-template-columns: 38px minmax(0, 1fr);
  gap: 10px;
  width: 100%;
  border: 1px solid transparent;
  border-radius: 10px;
  padding: 10px;
  background: transparent;
  color: #a8b3c7;
  text-align: left;
  cursor: pointer;
}

.nav-menu button:hover,
.nav-menu button.active {
  border-color: rgba(24, 144, 255, 0.45);
  background: rgba(24, 144, 255, 0.12);
  color: #f2f6ff;
}

.nav-mark {
  width: 38px;
  height: 38px;
  display: grid;
  place-items: center;
  border-radius: 10px;
  background: #191f2b;
  color: var(--quant-neutral);
  font-weight: 900;
}

.nav-menu strong,
.nav-menu small,
.service-card span,
.service-card strong,
.service-card small {
  display: block;
}

.nav-menu strong {
  font-size: 0.93rem;
}

.nav-menu small,
.service-card small {
  margin-top: 3px;
  color: #6f7d95;
  line-height: 1.35;
}

.service-card {
  margin-top: auto;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 12px;
  padding: 12px;
  background: var(--terminal-card);
}

.service-card span {
  color: #6f7d95;
  font-size: 0.76rem;
}

.service-card strong {
  margin-top: 5px;
  font-size: 1rem;
}

.ok {
  color: var(--quant-fall);
}

.bad {
  color: var(--quant-fall);
}

@media (max-width: 980px) {
  .sidebar {
    position: static;
    height: auto;
    border-right: 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.08);
  }

  .nav-menu {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
}

@media (max-width: 680px) {
  .nav-menu {
    grid-template-columns: 1fr;
  }
}
</style>
