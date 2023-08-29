function paginateTable(tableId, currentPage, rowsPerPage) {
    var table = document.getElementById(tableId);
    var rows = table.getElementsByTagName("tr");
    var totalPages = Math.ceil((rows.length - 1) / rowsPerPage); // 计算总页数

    // 隐藏所有行
    for (var i = 1; i < rows.length; i++) {
        rows[i].style.display = "none";
    }

    // 根据当前页和每页行数显示对应的行
    var startIndex = (currentPage - 1) * rowsPerPage + 1;
    var endIndex = Math.min(startIndex + rowsPerPage, rows.length);

    for (var j = startIndex; j < endIndex; j++) {
        rows[j].style.display = "";
    }

    // 更新分页按钮
    updatePaginationButtons(tableId, currentPage, totalPages);
}

function updatePaginationButtons(tableId, currentPage, totalPages) {
    var prevButton = document.getElementById(tableId + "-prev-button");
    var nextButton = document.getElementById(tableId + "-next-button");
    var currentPageLabel = document.getElementById(tableId + "-current-page-label");
    var totalPagesLabel = document.getElementById(tableId + "-total-pages-label");

    currentPageLabel.textContent = currentPage;
    totalPagesLabel.textContent = totalPages;

    // 根据当前页更新分页按钮状态
    if (currentPage === 1) {
        prevButton.disabled = true;
    } else {
        prevButton.disabled = false;
    }

    if (currentPage === totalPages) {
        nextButton.disabled = true;
    } else {
        nextButton.disabled = false;
    }
}

function goToPreviousPage(tableId) {
    var currentPage = parseInt(document.getElementById(tableId + "-current-page-label").textContent);
    paginateTable(tableId, currentPage - 1, 10);
}

function goToNextPage(tableId) {
    var currentPage = parseInt(document.getElementById(tableId + "-current-page-label").textContent);
    paginateTable(tableId, currentPage + 1, 10);
}
