const inputElement1 = document.getElementById("databaseSelect1");
const datalistElement1 = document.getElementById("databaseList1");

const inputElement2 = document.getElementById("databaseSelect2");
const datalistElement2 = document.getElementById("databaseList2");

fetch('../../static/config.json')
    .then(response => response.json())
    .then(jsonData => {
        function addOptionsToDatalist(datalistElement) {
            for (const databaseName in jsonData) {
                if (databaseName !== "registered_users") {
                    const optionElement = document.createElement("option");
                    optionElement.value = databaseName;
                    datalistElement.appendChild(optionElement);
                }
            }
        }

        addOptionsToDatalist(datalistElement1);
        addOptionsToDatalist(datalistElement2);

        // 将 datalist 与 input 进行关联
        inputElement1.setAttribute("list", "databaseList1");
        inputElement2.setAttribute("list", "databaseList2");
    })
    .catch(error => console.error('Error fetching ../../static/config.json:', error));

