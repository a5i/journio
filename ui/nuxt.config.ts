// Nuxt config for the Journio admin UI.
export default defineNuxtConfig({
  compatibilityDate: '2024-11-01',
  devtools: { enabled: true },
  modules: ['@nuxtjs/tailwindcss'],
  // The admin API runs on localhost:3001 (the Rust demo app).
  // Override at runtime with NUXT_PUBLIC_API_BASE.
  runtimeConfig: {
    public: {
      apiBase: 'http://localhost:3001',
    },
  },
  app: {
    head: {
      title: 'Journio Console',
      meta: [
        { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      ],
    },
  },
})
