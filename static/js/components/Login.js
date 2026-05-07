import { api, setToken } from '../api.js';

const { ref } = Vue;

export default {
  name: 'Login',

  setup() {
    const router   = VueRouter.useRouter();
    const username = ref('');
    const password = ref('');
    const error    = ref('');
    const loading  = ref(false);

    async function submit() {
      error.value   = '';
      loading.value = true;
      try {
        const res = await api.login(username.value, password.value);
        setToken(res.token);
        router.push('/dashboard');
      } catch {
        error.value = 'Credenziali non valide.';
      } finally {
        loading.value = false;
      }
    }

    return { username, password, error, loading, submit };
  },

  template: `
    <div class="login-page" style="min-height:100vh; display:flex; align-items:center; justify-content:center; background:#1a1a2e;">
      <div class="login-box" style="width:360px;">
        <div class="login-logo mb-3">
          <span class="font-weight-bold text-white" style="font-size:1.8rem; letter-spacing:2px;">
            <i class="fas fa-wave-square mr-2"></i>Waluigi
          </span>
        </div>
        <div class="card">
          <div class="card-body p-4">
            <p class="login-box-msg text-muted mb-3">Accedi alla console</p>

            <div v-if="error" class="alert alert-danger py-2 small">{{ error }}</div>

            <form @submit.prevent="submit">
              <div class="input-group mb-3">
                <input v-model="username" type="text" class="form-control"
                       placeholder="Username" autocomplete="username" required />
                <div class="input-group-append">
                  <div class="input-group-text"><i class="fas fa-user"></i></div>
                </div>
              </div>
              <div class="input-group mb-3">
                <input v-model="password" type="password" class="form-control"
                       placeholder="Password" autocomplete="current-password" required />
                <div class="input-group-append">
                  <div class="input-group-text"><i class="fas fa-lock"></i></div>
                </div>
              </div>
              <button type="submit" class="btn btn-primary btn-block" :disabled="loading">
                <span v-if="loading"><i class="fas fa-spinner fa-spin mr-1"></i>Accesso...</span>
                <span v-else><i class="fas fa-sign-in-alt mr-1"></i>Accedi</span>
              </button>
            </form>
          </div>
        </div>
      </div>
    </div>
  `,
};
