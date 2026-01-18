
// TODO these should probably be tied with events so they dont need to be exposed
function onPillDragStart(event) {
    event.dataTransfer.setData("text", event.target.id)
}

function onPillDrop(event) {
    event.preventDefault();
    const id = event.dataTransfer.getData("text")
    event.target.appendChild(document.getElementById(id))

    // issue change event to allow other components to update
    document.body.dispatchEvent(new Event("policy-attribute-changed"));
}

export {onPillDrop, onPillDragStart}