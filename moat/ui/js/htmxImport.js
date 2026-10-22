/*
 * htmx 2 ships ESM as its main entry, so a bare require() yields the module
 * namespace rather than htmx itself. Import the default and publish it on window,
 * which is where the json-enc extension (and any inline handler) looks for it.
 */
import htmx from 'htmx.org';

window.htmx = htmx;

export default htmx;
