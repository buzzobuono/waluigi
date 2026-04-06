export default {
  name: 'BaseButtonGroup',
  template: `
    <div 
      class="shadow-sm d-inline-flex align-items-center" 
      role="group" 
      style="gap: 4px; flex-wrap: nowrap; white-space: nowrap;"
    >
      <slot></slot>
    </div>
  `
};
