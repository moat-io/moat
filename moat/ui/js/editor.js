import { EditorState } from '@codemirror/state';
import { highlightSelectionMatches } from '@codemirror/search';
import { indentWithTab, history, defaultKeymap, historyKeymap } from '@codemirror/commands';
import { foldGutter, indentOnInput, indentUnit, bracketMatching, foldKeymap, syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language';
import { closeBrackets, autocompletion, closeBracketsKeymap, completionKeymap } from '@codemirror/autocomplete';
import { lineNumbers, highlightActiveLineGutter, highlightSpecialChars, drawSelection, dropCursor, rectangularSelection, crosshairCursor, highlightActiveLine, keymap, EditorView, ViewUpdate } from '@codemirror/view';
import { isDarkMode } from './dark-mode.js'

// Theme
import { oneDark } from "@codemirror/theme-one-dark";

// Language
import { javascript } from "@codemirror/lang-javascript";

let codemirrorEditor;


// to set the height of the editor and scroll, go to the bottom of this page:
// https://codemirror.net/examples/styling/

// pushes changes back to the hidden HTML element so HTMX can see them
function editorUpdateListener(v) {
    if (v.docChanged) {
        let contentElement = document.getElementById("dsl_content");
        contentElement.value = v.state.doc.toString()
    }
}

document.body.addEventListener("load_codemirror", function(evt){
    let readonly = evt.detail.readonly === "True";

    let extensions = [
        lineNumbers(),
        highlightActiveLineGutter(),
        highlightSpecialChars(),
        history(),
        foldGutter(),
        drawSelection(),
        indentUnit.of("    "),
        EditorState.allowMultipleSelections.of(true),
        indentOnInput(),
        bracketMatching(),
        closeBrackets(),
        autocompletion(),
        rectangularSelection(),
        crosshairCursor(),
        highlightActiveLine(),
        highlightSelectionMatches(),
        keymap.of([
            indentWithTab,
            ...closeBracketsKeymap,
            ...defaultKeymap,
            ...historyKeymap,
            ...foldKeymap,
            ...completionKeymap,
        ]),
        javascript(),
        syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
        EditorState.readOnly.of(readonly),
        EditorView.updateListener.of(editorUpdateListener)
    ];

    // enables dark mode
    if (isDarkMode()) {
        extensions.push(oneDark);
    }

    let contentElement = document.getElementById("dsl_content");

    let startState= EditorState.create({
        doc: contentElement.value,
        extensions
    });

    let editorElement = document.getElementById("editor");

    codemirrorEditor = new EditorView({
      state: startState,
      parent: editorElement
    });



    console.log("Codemirror loaded with config: readonly=", evt.detail.readonly);
});
