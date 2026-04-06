import BaseTable from './BaseTable.js';
import BaseButton from './BaseButton.js';
import BaseButtonGroup from './BaseButtonGroup.js';

export default {
  name: 'TaskTreeTable',
  components: { BaseTable, BaseButton, BaseButtonGroup },
  props: ['tasks', 'colors'],
  emits: ['reset', 'delete', 'show-logs'],
  
  setup(props) {
    const columns = [
      { key: 'id', label: 'Task ID' },
      { key: 'status', label: 'Status' },
      { key: 'params', label: 'Parameters' },
      { key: 'last_update', label: 'Updated' },
      { key: 'actions', label: 'Actions', class: 'text-right pr-3' }
    ];

    const flatten = (items, parentId = null, level = 0) => {
      let result = [];
      const children = items.filter(t => 
        String(t.parent_id) === String(parentId) || 
        (!parentId && !items.some(p => p.id === t.parent_id))
      );
      
      children.forEach(child => {
        result.push({ ...child, _level: level });
        const subChildren = items.filter(t => String(t.parent_id) === String(child.id));
        if (subChildren.length > 0) {
          result = result.concat(flatten(items, child.id, level + 1));
        }
      });
      return result;
    };

    const flatTasks = Vue.computed(() => flatten(props.tasks));

    return { columns, flatTasks };
  },

  template: `
    <base-table :columns="columns" :items="flatTasks">
      
      <template #cell(id)="{ item }">
        <div :style="'padding-left:' + (item._level * 20) + 'px'" class="py-1 text-nowrap">
          <span v-if="item._level > 0" class="text-muted mr-1">└─ </span>
          <a href="#" 
             @click.prevent="$emit('show-logs', item.id)" 
             class="wl-accent font-weight-bold">
            {{ item.id }}
          </a>
        </div>
      </template>

      <template #cell(status)="{ item }">
        <span class="badge shadow-sm" 
              :style="{ background: (colors && colors[item.status]) || '#6c757d', color: '#fff', minWidth: '70px' }">
          {{ item.status }}
        </span>
      </template>

      <template #cell(params)="{ item }">
        <span class="text-muted small">{{ item.params || '—' }}</span>
      </template>

      <template #cell(actions)="{ item }">
        <base-button-group>
          <base-button 
            icon="fas fa-undo" 
            color="outline-warning" 
            title="Reset Task"
            @click.stop="$emit('reset', item.id)" 
          />
          <base-button 
            icon="fas fa-trash" 
            color="outline-danger" 
            title="Delete Task"
            @click.stop="$emit('delete', item.id)" 
          />
        </base-button-group>
      </template>

    </base-table>
  `
};
