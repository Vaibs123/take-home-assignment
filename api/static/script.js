$(document).ready(function() {
    var currentPage = 1;

    function fetchData(page) {
        $.ajax({
            url: '/covid-stats/get-covid-stats?page=' + page,
            dataType: 'json',
            success: function(response) {
                var data = response.data;
                var totalPages = response.total_pages;

                // Render the data in the data container
                var dataContainer = $('#data-container');
                dataContainer.empty();
                data.forEach(function(item) {
                    var row = $('<div>').text(JSON.stringify(item));
                    dataContainer.append(row);
                });

                // Render pagination buttons
                var paginationContainer = $('#pagination-container');
                paginationContainer.empty();

                if (response.prev_page !== null) {
                    var prevButton = $('<button>').text('Previous');
                    prevButton.on('click', function() {
                        fetchData(response.prev_page);
                    });
                    paginationContainer.append(prevButton);
                }

                for (var i = 1; i <= totalPages; i++) {
                    var pageButton = $('<button>').text(i);
                    if (i === page) {
                        pageButton.addClass('active');
                    }
                    pageButton.on('click', function() {
                        fetchData(parseInt($(this).text()));
                    });
                    paginationContainer.append(pageButton);
                }

                if (response.next_page !== null) {
                    var nextButton = $('<button>').text('Next');
                    nextButton.on('click', function() {
                        fetchData(response.next_page);
                    });
                    paginationContainer.append(nextButton);
                }
            }
        });
    }

    // Initial data fetch
    fetchData(currentPage);
});
