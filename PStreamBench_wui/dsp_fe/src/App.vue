<template>
  <v-app>
    <div id="app" >
      <router-view />
    </div>
    <!--Snackbar global variable is used here-->
    <v-snackbar
      v-model="snackbar.view"
      :timeout="snackbar.timeout"
      :color="snackbar.color"
    >
      {{ snackbar.text }}

      <template #action="{ attrs }">
        <v-btn text v-bind="attrs" @click="snackbar.view = false">
          Close
        </v-btn>
      </template>
    </v-snackbar>
  
    
   
  </v-app>
</template>

<script>

export default {
  name: 'App',

  components: {
  },

  data: () => ({
    //
  }),
  computed:{
    snackbar: {
			get() {
				return this.$store.state.snackbar;
			},
			set(value) {
				this.$store.commit('setSnackbar', value);
			},
		},
    loader: {
			get() {
        
				return this.$store.state.loader;
			},
			set(value) {
				this.$store.commit('setLoader', value);
			},
		},
  }
};
</script>
<style>
/* vietnamese */
@font-face {
  font-family: 'Quicksand';
  font-style: normal;
  font-weight: 400;
  font-display: swap;
  src: url(assets/quicksand_vt.woff2) format('woff2');
  unicode-range: U+0102-0103, U+0110-0111, U+0128-0129, U+0168-0169, U+01A0-01A1, U+01AF-01B0, U+1EA0-1EF9, U+20AB;
}
/* latin-ext */
@font-face {
  font-family: 'Quicksand';
  font-style: normal;
  font-weight: 400;
  font-display: swap;
  src: url(assets/quicksand_ltext.woff2) format('woff2');
  unicode-range: U+0100-024F, U+0259, U+1E00-1EFF, U+2020, U+20A0-20AB, U+20AD-20CF, U+2113, U+2C60-2C7F, U+A720-A7FF;
}
/* latin */
@font-face {
  font-family: 'Quicksand';
  font-style: normal;
  font-weight: 400;
  font-display: swap;
  src: url(assets/quicksand_lt.woff2) format('woff2');
  unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+2000-206F, U+2074, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD;
}

/* Previously only `div { font-family: "Quicksand" }` -- Quicksand was
   declared but most Vuetify components render as other tags (button,
   span, h1-h6, input, td...) and silently fell back to Vuetify's default
   Roboto, mixing two fonts across the page. Applying it to .v-application
   (Vuetify's own root wrapper class) and overriding with !important
   matches how Vuetify expects a global font override to be done. */
html, body, #app, .v-application {
  font-family: "Quicksand", sans-serif !important;
}

/* Minor look-and-feel refresh (2026-10-15): rounded corners + softer
   shadows read as more modern than Vuetify 2's flat default card/button
   style; kept subtle rather than a full redesign. */
.v-card, .v-btn, .v-sheet, .v-text-field .v-input__slot, .v-select .v-input__slot {
  border-radius: 10px !important;
}
.v-btn {
  text-transform: none !important;
  letter-spacing: normal !important;
}

/* Spacing/elevation pass: Vuetify's default card padding reads cramped,
   and nothing responded on hover except the few places that already had
   a manual v-hover wired up. A gentle lift + shadow transition on every
   card (not just the hand-wired ones) makes the whole app feel more
   responsive without touching any component's markup. */
.v-card__text, .v-card__title {
  padding: 20px 24px !important;
}
.v-card {
  transition: box-shadow 0.2s ease, transform 0.2s ease;
}
.v-card:hover {
  transform: translateY(-2px);
}

/* Typography hierarchy: previously every card title/body text rendered
   at roughly the same weight/size, so nothing visually signaled "this is
   the heading" vs. "this is detail text" beyond raw font-size. Bolder,
   slightly larger titles; lighter/muted secondary text. */
.v-card__title {
  font-weight: 700 !important;
  font-size: 1.25rem !important;
}
.v-card__subtitle, .v-card__text {
  color: rgba(0, 0, 0, 0.68);
}
.v-card__text b, .v-card__text strong {
  color: rgba(0, 0, 0, 0.87);
}

/* Defensive fix for the reported "background looks short when zoomed
   out" issue: no explicit page-height rule existed anywhere, so the app
   relied entirely on Vuetify's own .v-application sizing. Forcing
   min-height: 100% up the whole chain (html -> body -> #app ->
   .v-application) means a zoomed-out viewport still has a parent tall
   enough to fill, regardless of which element the original gap was in. */
html, body {
  height: 100%;
}
#app, .v-application {
  min-height: 100vh;
  min-height: 100%;
}
