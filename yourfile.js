(function() {
    const myConnector = tableau.makeConnector();

    myConnector.getSchema = function(schemaCallback) {
        // Define your schema with all specified columns
        const cols = [
            { id: "month", alias: "Month", dataType: tableau.dataTypeEnum.string },
            { id: "town", alias: "Town", dataType: tableau.dataTypeEnum.string },
            { id: "flat_type", alias: "Flat Type", dataType: tableau.dataTypeEnum.string },
            { id: "block", alias: "Block", dataType: tableau.dataTypeEnum.string },
            { id: "street_name", alias: "Street Name", dataType: tableau.dataTypeEnum.string },
            { id: "storey_range", alias: "Storey Range", dataType: tableau.dataTypeEnum.string },
            { id: "floor_area_sqm", alias: "Floor Area (sqm)", dataType: tableau.dataTypeEnum.float },
            { id: "flat_model", alias: "Flat Model", dataType: tableau.dataTypeEnum.string },
            { id: "lease_commence_date", alias: "Lease Commence Date", dataType: tableau.dataTypeEnum.int },
            { id: "remaining_lease", alias: "Remaining Lease", dataType: tableau.dataTypeEnum.string },
            { id: "resale_price", alias: "Resale Price", dataType: tableau.dataTypeEnum.float }
        ];

        const tableSchema = {
            id: "ResaleFlatPrices",
            alias: "Resale Flat Prices Dataset",
            columns: cols
        };

        schemaCallback([tableSchema]);
    };

    myConnector.getData = function(table, doneCallback) {
        const baseUrl = "https://s3.ap-southeast-1.amazonaws.com/table-downloads-ingest.data.gov.sg/d_8b84c4ee58e3cfc0ece0d773c8ca6abc/6f8109f7bce05c219b3825a999cc7f3a02cbc19fe536138a5eaf86bfe6d8711f.csv";
        let offset = 0;
        const limit = 100;
        let hasMoreData = true;

        // Function to fetch data in chunks
        const fetchData = function() {
            const url = `${baseUrl}?limit=${limit}&offset=${offset}`;

            fetch(url)
                .then(response => response.json())
                .then(data => {
                    const rows = data.results.map(item => {
                        return {
                            month: item.month,
                            town: item.town,
                            flat_type: item.flat_type,
                            block: item.block,
                            street_name: item.street_name,
                            storey_range: item.storey_range,
                            floor_area_sqm: item.floor_area_sqm,
                            flat_model: item.flat_model,
                            lease_commence_date: item.lease_commence_date,
                            remaining_lease: item.remaining_lease,
                            resale_price: item.resale_price
                        };
                    });

                    table.appendRows(rows);
                    offset += limit;
                    hasMoreData = data.results.length === limit;

                    // Continue fetching if there are more rows
                    if (hasMoreData) {
                        fetchData();
                    } else {
                        doneCallback();
                    }
                })
                .catch(error => {
                    console.error("Error fetching data: ", error);
                    doneCallback();
                });
        };

        fetchData();
    };

    tableau.registerConnector(myConnector);
})();
