/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{vue,js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        panel: '#10161f',
        shell: '#060b12',
        edge: '#243243',
        accent: '#1cff88',
      },
      boxShadow: {
        glow: '0 0 22px rgba(28, 255, 136, 0.6)',
      },
    },
  },
  plugins: [],
}
