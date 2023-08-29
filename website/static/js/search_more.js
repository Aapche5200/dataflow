// search.js

var searchResults = [];

function updateCombinedResults() {
    var combinedResultsDiv = document.getElementById("combined-results");
    combinedResultsDiv.innerHTML = "Combined Results: " + searchResults.join(", ");
}

function filterTable(inputId, tableId, columnIndex) {
    var input = document.getElementById(inputId);
    var filter = input.value.toUpperCase();
    var table = document.getElementById(tableId);
    var tr = table.getElementsByTagName("tr");

    searchResults[columnIndex] = filter;
    updateCombinedResults();
}

function clearSearch(tableId) {
    searchResults = [];
    updateCombinedResults();
    applySearch(tableId);
}

function executeSearch(tableId) {
    var table = document.getElementById(tableId);
    var tr = table.getElementsByTagName("tr");

    for (var i = 0; i < tr.length; i++) {
        var displayRow = true;

        for (var j = 0; j < searchResults.length; j++) {
            var td = tr[i].getElementsByTagName("td")[j];
            if (td) {
                var txtValue = td.textContent || td.innerText;
                if (searchResults[j] && txtValue.toUpperCase().indexOf(searchResults[j]) === -1) {
                    displayRow = false;
                    break;
                }
            }
        }

        tr[i].style.display = displayRow ? "" : "none";
    }
}
