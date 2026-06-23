/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './components/**/*.{vue,js,ts}',
    './pages/**/*.{vue,js,ts}',
    './app.vue',
  ],
  theme: {
    extend: {
      colors: {
        // Map Journio workflow statuses to Tailwind color tokens.
        success: '#10b981',
        error: '#ef4444',
        pending: '#3b82f6',
        enqueued: '#8b5cf6',
        cancelled: '#6b7280',
        delayed: '#f59e0b',
      },
    },
  },
  plugins: [],
}
