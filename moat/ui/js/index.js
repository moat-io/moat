import './sidebar.js';
// import './charts.js';
import './dark-mode.js';
import './flowbite.js'
import './toast.js'
import './editor.js'
import 'htmx.org';
import './htmxImport.js'
import 'htmx-ext-json-enc'
import moment from "moment";
import {onPillDrop, onPillDragStart} from './policyBuilder.js'

document.body.addEventListener("initialiseFlowbite", function(evt){
    initFlowbite();
    moment_render_all();
    console.log("Triggered initialiseFlowbite from HX-Trigger header")
})

// HTMX expects some functions to be available on the global (window) scope
window.onPillDrop = onPillDrop;
window.onPillDragStart = onPillDragStart;

window.formatDatetime = function formatDatetime(dt) {
    console.log(moment(dt).format())
    return moment(dt).format()
}

function moment_render_all() {
    const elements = document.querySelectorAll('.render-moment');
    elements.forEach(function (element){
        const formatName = element.dataset.renderMoment;
        let formatString;
        if (formatName === "date") {
            formatString = 'DD/MM/YYYY'
        }
        else if (formatName === "time") {
            formatString = 'HH:mm:ss.SS Z'
        }
        else if (formatName === "datetime") {
            formatString = 'DD/MM/YYYY HH:mm:ss.SS Z'
        }
        else if (formatName === "offset") {
            formatString = 'Z'
        }
        element.innerHTML = moment(element.innerHTML).format(formatString)
    })
}