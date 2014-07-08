var records = function(cb) {
    $(".persons tbody tr").each(function() { $(".alias", this).length > 0 || cb.apply(this); });
};

var authorPattern = /(Schriftsteller)|(Autor)/i, professions = {}, maxProfessions = 0;
records(function() {
    $(".profession span", this).each(function() {
        var p = $(this).text();
        if (authorPattern.exec(p)) return;
        maxProfessions = Math.max(maxProfessions, (professions[p] = (professions[p] || 0) + 1));
    });
});


var professionNames = d3.keys(professions),
    professionScale = d3.scale.log().base(20).domain([1, maxProfessions]).range([1, 155]);

professionNames.sort(d3.ascending);

var professionChart = d3.select("svg.professions")
    .attr("width", 960)
    .attr("height", Math.ceil(professionNames.length / 6) * 25);

var professionBar = professionChart
    .selectAll("g")
    .data(professionNames)
    .enter().append("g")
    .attr("transform", function(d, i) {
        var col = i % 6, row = Math.floor(i / 6);
        return "translate(" + (col * 160) + ", " + (row * 25) + ")";
    });

professionBar.append("rect")
    .attr("width", function(d) { return professionScale(professions[d]); })
    .attr("height", 20);

professionBar.append("text")
    .attr("x", 0)
    .attr("y", 10)
    .attr("dx", "0.25em")
    .attr("dy", "0.35em")
    .text(function(d) {
        return (d.length > 15 ? d.substring(0, 15)  + "." : d) + " (" + professions[d] + ")";
    });

var liveData = { male: [], female: [], total: [], maxAlive: 0 };

for (var year = 0; year < 150; year++) liveData.male[year] = liveData.female[year] = liveData.total[year] = 0;

records(function() {
    var liveDataEl = $(".live-data", this).first(),
        born = parseInt(liveDataEl.data("born") || "0"),
        died = parseInt(liveDataEl.data("died") || "0");

    if (born || died) {
        born = Math.max(1800, born || (died - 20)) - 1800;
        died = Math.min(1949, died || (born + 20)) - 1800;

        var nameSpan = $(".name span", this),
            isMale = nameSpan.hasClass("male"),
            isFemale = nameSpan.hasClass("female");
        for (var year = born; year <= died; year++) {
            liveData.maxAlive = Math.max(liveData.maxAlive, ++liveData.total[year]);
            isMale && ++liveData.male[year];
            isFemale && ++liveData.female[year];
        }
    }
});

var yearScale = d3.scale.linear().domain([0, 150]).range([0, 900]),
    yearAxis = d3.svg.axis().orient("bottom")
        .scale(yearScale)
        .tickFormat(function(d) { return "" + (1800 + d); }),
    aliveScale = d3.scale.linear().domain([0, liveData.maxAlive]).range([300, 0]),
    aliveAxis = d3.svg.axis().orient("left")
        .scale(aliveScale);

var liveDataChart = d3.select("svg.live-data")
    .attr("width", 960)
    .attr("height", 360)
    .append("g")
    .attr("transform", "translate(30, 30)");

liveDataChart.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0, 300)")
    .call(yearAxis);

liveDataChart.append("g")
    .attr("class", "y axis")
    .attr("transform", "translate(0, 0)")
    .call(aliveAxis);

["total", "male", "female"].forEach(function(dataType) {
    liveDataChart
        .append("path")
        .datum(d3.range(150))
        .attr("class", "area " + dataType)
        .attr("d", d3.svg.area()
            .x(function(d) { return yearScale(d); })
            .y0(300)
            .y1(function(d) {  return aliveScale(liveData[dataType][d]); }));
});