import { api, setToken } from '../api.js';
import BaseButton from './BaseButton.js';

const { ref } = Vue;

export default {
  name: 'Login',
  components: { BaseButton },

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
    <div class="login-page d-flex align-items-center justify-content-center min-vh-100">
      <div class="login-box">

        <div class="login-logo">
          <i class="fas fa-wave-square mr-2"></i><b>Waluigi</b>
        </div>

        <div class="card">
          <div class="card-body login-card-body">

            <p class="login-box-msg">Accedi alla console</p>

            <div v-if="error" class="alert alert-danger py-2 small">{{ error }}</div>

            <form @submit.prevent="submit">
              <div class="input-group mb-3">
                <input v-model="username" type="text" class="form-control"
                       placeholder="Username" autocomplete="username" required />
                <div class="input-group-append">
                  <div class="input-group-text"><i class="fas fa-user"></i></div>
                </div>
              </div>

              <div class="input-group mb-4">
                <input v-model="password" type="password" class="form-control"
                       placeholder="Password" autocomplete="current-password" required />
                <div class="input-group-append">
                  <div class="input-group-text"><i class="fas fa-lock"></i></div>
                </div>
              </div>

              <base-button
                label="Accedi"
                icon="fas fa-sign-in-alt"
                color="primary"
                size="md"
                :loading="loading"
                :disabled="loading"
                class="btn-block"
                @click="submit"
              />
            </form>

          </div>
        </div>

      </div>
    </div>
  `,
};
