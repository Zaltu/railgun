import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  envPrefix: "RG_",
  plugins: [react()],
  server: {
    port: 5174
  }
})
