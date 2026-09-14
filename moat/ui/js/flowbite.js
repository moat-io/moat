/*
 * Flowbite is pulled from the package rather than vendored, so its JS stays in
 * lockstep with the `@plugin "flowbite/plugin"` styles in css/input.css.
 *
 * `initFlowbite` is published on window because htmx re-runs it after every swap
 * (see the initialiseFlowbite listener in index.js) to bind newly inserted
 * dropdowns, drawers and modals.
 */
import { initFlowbite } from 'flowbite';

window.initFlowbite = initFlowbite;

export { initFlowbite };
