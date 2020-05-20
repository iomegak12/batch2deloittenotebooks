-- Databricks notebook source
CREATE WIDGET DROPDOWN OrderLocation
DEFAULT "Bangalore"
CHOICES
  SELECT DISTINCT billingaddress AS OrderLocation 
  FROM CaseStudy1DB.ProcessedOrders

-- COMMAND ----------

SELECT customertype, SUM(orderamount)
FROM CaseStudy1DB.ProcessedOrders
WHERE billingaddress = getArgument("OrderLocation")
GROUP BY customertype

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ![Deloitte](https://upload.wikimedia.org/wikipedia/en/2/2b/DeloitteNewSmall.png)

-- COMMAND ----------

SELECT statusdesc AS CustomerStatus, SUM(orderamount) AS OrderAmount
FROM CaseStudy1DB.ProcessedOrders
WHERE billingaddress = getArgument("OrderLocation")
GROUP BY statusdesc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
-- MAGIC 
-- MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)
-- MAGIC 
-- MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC 
-- MAGIC val colorsRDD = sc.parallelize(
-- MAGIC 	Array(
-- MAGIC 		(197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), 
-- MAGIC 		(230,245,208), (184,225,134), (127,188,65), (77,146,33)))
-- MAGIC 
-- MAGIC val colors = colorsRDD.collect()
-- MAGIC 
-- MAGIC 
-- MAGIC displayHTML(s"""
-- MAGIC <!DOCTYPE html>
-- MAGIC <meta charset="utf-8">
-- MAGIC <style>
-- MAGIC 
-- MAGIC path {
-- MAGIC   fill: yellow;
-- MAGIC   stroke: #000;
-- MAGIC }
-- MAGIC 
-- MAGIC circle {
-- MAGIC   fill: #fff;
-- MAGIC   stroke: #000;
-- MAGIC   pointer-events: none;
-- MAGIC }
-- MAGIC 
-- MAGIC .PiYG .q0-9{fill:rgb${colors(0)}}
-- MAGIC .PiYG .q1-9{fill:rgb${colors(1)}}
-- MAGIC .PiYG .q2-9{fill:rgb${colors(2)}}
-- MAGIC .PiYG .q3-9{fill:rgb${colors(3)}}
-- MAGIC .PiYG .q4-9{fill:rgb${colors(4)}}
-- MAGIC .PiYG .q5-9{fill:rgb${colors(5)}}
-- MAGIC .PiYG .q6-9{fill:rgb${colors(6)}}
-- MAGIC .PiYG .q7-9{fill:rgb${colors(7)}}
-- MAGIC .PiYG .q8-9{fill:rgb${colors(8)}}
-- MAGIC 
-- MAGIC </style>
-- MAGIC <body>
-- MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
-- MAGIC <script>
-- MAGIC 
-- MAGIC var width = 960,
-- MAGIC     height = 500;
-- MAGIC 
-- MAGIC var vertices = d3.range(100).map(function(d) {
-- MAGIC   return [Math.random() * width, Math.random() * height];
-- MAGIC });
-- MAGIC 
-- MAGIC var svg = d3.select("body").append("svg")
-- MAGIC     .attr("width", width)
-- MAGIC     .attr("height", height)
-- MAGIC     .attr("class", "PiYG")
-- MAGIC     .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });
-- MAGIC 
-- MAGIC var path = svg.append("g").selectAll("path");
-- MAGIC 
-- MAGIC svg.selectAll("circle")
-- MAGIC     .data(vertices.slice(1))
-- MAGIC   .enter().append("circle")
-- MAGIC     .attr("transform", function(d) { return "translate(" + d + ")"; })
-- MAGIC     .attr("r", 2);
-- MAGIC 
-- MAGIC redraw();
-- MAGIC 
-- MAGIC function redraw() {
-- MAGIC   path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
-- MAGIC   path.exit().remove();
-- MAGIC   path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
-- MAGIC }
-- MAGIC 
-- MAGIC </script>
-- MAGIC   """)