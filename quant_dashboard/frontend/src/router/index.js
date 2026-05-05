export const routes = [
  { path: '/v3/dashboard', section: 'dashboard', label: 'Dashboard 总览' },
  { path: '/v4/dashboard', section: 'dashboard', label: 'Dashboard 总览' },
  { path: '/v5/account', section: 'account', label: 'V5.0 资金池' },
  { path: '/', section: 'dashboard', label: 'Dashboard 总览' },
]

export function resolveInitialSection(pathname = window.location.pathname) {
  const match = routes.find((route) => route.path === pathname)
  return match?.section || 'dashboard'
}
