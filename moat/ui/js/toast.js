

document.body.addEventListener("toast_success", function(evt){
    showSuccessToast(evt.detail.message);
})

function showSuccessToast(message){
    const toastElement = document.getElementById('toast-success');
    toastElement.classList.remove('invisible');

    const toastElementContent = document.getElementById('toast-success-content');
    toastElementContent.innerText = message

    setTimeout(hideSuccessToast, 1500);
}

function hideSuccessToast(){
    const $targetEl = document.getElementById('toast-success');
    $targetEl.classList.add('invisible');
}