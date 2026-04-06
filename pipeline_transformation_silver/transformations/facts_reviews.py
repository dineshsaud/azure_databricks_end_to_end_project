import dlt
from pyspark.sql.functions import udf, col, get_json_object
from pyspark.sql.types import StringType
import openai

# Define UDF
def analyze_review(review_text):
    client = openai.AzureOpenAI(
        api_key="YOUR_AZURE_OPENAI_KEY",
        api_version="2024-08-01-preview",
        azure_endpoint="https://dbx-openai.openai.azure.com"
    )
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": 
            'Analyze the following review and return ONLY a valid JSON object with this exact structure: '
            '{"sentiment": "<positive/neutral/negative>", '
            '"issue_delivery": "<true/false>", '
            '"issue_delivery_reason": "<reason or empty string>", '
            '"issue_food_quality": "<true/false>", '
            '"issue_food_quality_reason": "<reason or empty string>", '
            '"issue_pricing": "<true/false>", '
            '"issue_pricing_reason": "<reason or empty string>", '
            '"issue_portion_size": "<true/false>", '
            '"issue_portion_size_reason": "<reason or empty string>"}. '
            'Return ONLY the JSON, no other text. '
            'Review text: ' + review_text
        }]
    )
    return response.choices[0].message.content

analyze_review_udf = udf(analyze_review, StringType())

# DLT table
@dlt.table(name="fact_reviews", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_sentiment", "sentiment IN ('positive', 'neutral', 'negative')")
@dlt.expect_or_drop("non_negative_rating", "rating >= 0")
def fact_reviews():
    return (
        dlt.read_stream("01_bronze.reviews")
        .withColumn("analysis_json", analyze_review_udf(col("review_text")))
        .select(
            col("review_id"),
            col("order_id"),
            col("customer_id"),
            col("restaurant_id"),
            col("rating"),
            col("review_text"),
            col("analysis_json"),
            get_json_object(col("analysis_json"), "$.sentiment").alias("sentiment"),
            get_json_object(col("analysis_json"), "$.issue_delivery").alias("issue_delivery"),
            get_json_object(col("analysis_json"), "$.issue_delivery_reason").alias("issue_delivery_reason"),
            get_json_object(col("analysis_json"), "$.issue_food_quality").alias("issue_food_quality"),
            get_json_object(col("analysis_json"), "$.issue_food_quality_reason").alias("issue_food_quality_reason"),
            get_json_object(col("analysis_json"), "$.issue_pricing").alias("issue_pricing"),
            get_json_object(col("analysis_json"), "$.issue_pricing_reason").alias("issue_pricing_reason"),
            get_json_object(col("analysis_json"), "$.issue_portion_size").alias("issue_portion_size"),
            get_json_object(col("analysis_json"), "$.issue_portion_size_reason").alias("issue_portion_size_reason"),
            col("review_timestamp")
        )
    )