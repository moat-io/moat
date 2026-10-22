/*
 * Table chrome that is page-level rather than view-level: the density preference,
 * the search-box affordances and the keyboard shortcuts.
 *
 * These bind once against the document and survive htmx swaps, so nothing needs
 * re-wiring when a view is replaced.
 */

const DENSITY_KEY = 'moat-density';
const COMPACT_CLASS = 'density-compact';

function applyDensity(density) {
    document.documentElement.classList.toggle(COMPACT_CLASS, density === 'compact');
}

function currentDensity() {
    return document.documentElement.classList.contains(COMPACT_CLASS) ? 'compact' : 'comfortable';
}

function toggleDensity() {
    const next = currentDensity() === 'compact' ? 'comfortable' : 'compact';
    localStorage.setItem(DENSITY_KEY, next);
    applyDensity(next);
}

/* The "/" hint is swapped for a clear button once there is a term to clear. */
function syncSearchAffordances(input) {
    const wrapper = input.closest('.relative');
    if (!wrapper) {
        return;
    }

    const hasValue = input.value.length > 0;
    const hint = wrapper.querySelector('[data-moat-search-hint]');
    const clear = wrapper.querySelector('[data-moat-search-clear]');

    if (hint) {
        hint.classList.toggle('hidden', hasValue || document.activeElement === input);
        hint.classList.toggle('sm:inline-block', !hasValue && document.activeElement !== input);
    }
    if (clear) {
        clear.classList.toggle('hidden', !hasValue);
    }
}

function syncAllSearchAffordances() {
    document.querySelectorAll('[data-moat-search]').forEach(syncSearchAffordances);
}

window.moatClearSearch = function moatClearSearch(prefix) {
    const input = document.getElementById(prefix + '-search');
    if (!input) {
        return;
    }
    input.value = '';
    syncSearchAffordances(input);
    // htmx listens for keyup on the input; dispatching one re-runs the query
    input.dispatchEvent(new Event('search', {bubbles: true}));
    input.focus();
};

function visibleSearchInput() {
    return Array.from(document.querySelectorAll('[data-moat-search]'))
        .find((input) => input.offsetParent !== null);
}

function isTypingTarget(element) {
    if (!element) {
        return false;
    }
    return element.isContentEditable
        || ['INPUT', 'TEXTAREA', 'SELECT'].includes(element.tagName);
}

applyDensity(localStorage.getItem(DENSITY_KEY));

document.addEventListener('click', (event) => {
    if (event.target.closest('[data-moat-density-toggle]')) {
        toggleDensity();
    }
});

document.addEventListener('input', (event) => {
    if (event.target.matches('[data-moat-search]')) {
        syncSearchAffordances(event.target);
    }
});

document.addEventListener('focusin', (event) => {
    if (event.target.matches('[data-moat-search]')) {
        syncSearchAffordances(event.target);
    }
});

document.addEventListener('focusout', (event) => {
    if (event.target.matches('[data-moat-search]')) {
        setTimeout(() => syncSearchAffordances(event.target), 0);
    }
});

document.addEventListener('keydown', (event) => {
    // "/" focuses search, unless the user is already typing somewhere
    if (event.key === '/' && !isTypingTarget(event.target)) {
        const input = visibleSearchInput();
        if (input) {
            event.preventDefault();
            input.focus();
            input.select();
        }
        return;
    }

    if (event.key === 'Escape' && event.target.matches('[data-moat-search]')) {
        if (event.target.value) {
            event.preventDefault();
            const prefix = event.target.id.replace(/-search$/, '');
            window.moatClearSearch(prefix);
        } else {
            event.target.blur();
        }
    }
});

// views arrive via htmx, so re-sync after every swap
document.body.addEventListener('htmx:afterSwap', syncAllSearchAffordances);
document.addEventListener('DOMContentLoaded', syncAllSearchAffordances);

export {applyDensity, toggleDensity};
