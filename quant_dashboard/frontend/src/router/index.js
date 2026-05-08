export const routes = [
  { path: '/v3/dashboard', section: 'dashboard', label: 'Dashboard 总览' },
  { path: '/v4/dashboard', section: 'dashboard', label: 'Dashboard 总览' },
  { path: '/v5/ledger', section: 'ledger', label: '真实账本数据' },
  { path: '/v5/backtest', section: 'backtest', label: '回测模拟数据' },
  { path: '/v5/pushplus', section: 'pushplus', label: 'PushPlus Token 管理' },
  { path: '/v5/account', section: 'account', label: 'V5.0 资金池' },
  { path: '/', section: 'dashboard', label: 'Dashboard 总览' },
]

export function resolveInitialSection(pathname = window.location.pathname) {
  const match = routes.find((route) => route.path === pathname)
  return match?.section || 'dashboard'
}
