export default {
  name: 'BaseTable',
  props: {
    columns: { type: Array, required: true },
    items: { type: Array, required: true },
    hover: { type: Boolean, default: true },
    striped: { type: Boolean, default: false }
  },
  template: `
    <div class="table-responsive">
      <table :class="['table table-sm mb-0', { 'table-hover': hover, 'table-striped': striped }]">
        <thead>
          <tr>
            <th v-for="col in columns" :key="col.key" :class="col.class">
              {{ col.label }}
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in items" :key="index">
            <td v-for="col in columns" :key="col.key" :class="col.class">
              <slot :name="'cell(' + col.key + ')'" :item="item">
                {{ item[col.key] }}
              </slot>
            </td>
          </tr>
          <tr v-if="!items.length">
            <td :colspan="columns.length" class="text-center p-4 text-muted">
              Nessun dato disponibile
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  `
};
