const rows = document.querySelectorAll("tr:not(:first-child)");

rows.forEach(row => {
    const editButton = row.querySelector(".editButton");
    const saveButton = row.querySelector(".saveButton");
    const cancelButton = row.querySelector(".cancelButton"); // 获取取消按钮
    const optionsContainer = row.querySelector(".optionsContainer");
    const checkboxOptions = row.querySelectorAll(".checkboxOption");
    const selectedOptionsInput = row.querySelector(".selectedOptions");
    const searchInput = row.querySelector(".searchInput");
    const searchIcon = row.querySelector(".searchIcon");

    const defaultDisplayLimit = 5; // Number of options to display by default

    let selectedOptions = [];

    editButton.addEventListener("click", () => {
        optionsContainer.style.display = "block";
        searchInput.style.display = "block";
        searchInput.focus();
    });

    saveButton.addEventListener("click", () => {
        selectedOptions = Array.from(checkboxOptions)
            .filter(option => option.checked)
            .map(option => option.value);

        selectedOptionsInput.value = selectedOptions.join(", ");
        optionsContainer.style.display = "none";
        searchInput.style.display = "none";
    });

    checkboxOptions.forEach((option, index) => {
        option.addEventListener("change", () => {
            selectedOptions = Array.from(checkboxOptions)
                .filter(option => option.checked)
                .map(option => option.value);
        });

        if (index >= defaultDisplayLimit) {
            option.parentElement.style.display = "none";
        }
    });

    selectedOptionsInput.addEventListener("click", () => {
        optionsContainer.style.display = "block";
        searchInput.style.display = "block";
        searchInput.focus();
    });

    searchIcon.addEventListener("click", () => {
        searchInput.style.display = "block";
        searchInput.focus();
    });

    searchInput.addEventListener("input", () => {
        const searchValue = searchInput.value.toLowerCase();
        checkboxOptions.forEach(option => {
            const label = option.parentElement.textContent.toLowerCase();
            if (label.includes(searchValue)) {
                option.parentElement.style.display = "block";
            } else {
                option.parentElement.style.display = "none";
            }
        });
    });

    optionsContainer.addEventListener("blur", () => {
        optionsContainer.style.display = "none";
        searchInput.style.display = "none";
    });

cancelButton.addEventListener("click", () => {
    // 取消按钮点击事件：取消未被选中的选项并隐藏选项容器
    checkboxOptions.forEach(option => {
        if (!option.checked) {
            option.checked = selectedOptions.includes(option.value);
        }
    });
    optionsContainer.style.display = "none";
    searchInput.style.display = "none";
});

});

