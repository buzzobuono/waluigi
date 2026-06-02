// Shared reactive namespace state — imported by any component that needs it
export const nsStore = Vue.reactive({
  selected:  '',
  available: [],
});
