export const routes = [
  { path: '/v3/dashboard', section: 'v3', label: 'V3.2 量化指挥中心' },
  { path: '/', section: 'dashboard', label: 'Dashboard 总览' },
]

export function resolveInitialSection(pathname = window.location.pathname) {
  const match = routes.find((route) => route.path === pathname)
  return match?.section || 'dashboard'
}
