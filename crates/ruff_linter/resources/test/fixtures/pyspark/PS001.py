import pyspark.sql.functions as F

# Complexity = 5
in_service_filter = F.when( (F.col('prod_status') == 'Delivered')
                                | (((F.datediff('deliveryDate_actual', 'current_date') < 0)
                                  & ((F.col('currentRegistration') != '')
                                     | ((F.datediff('deliveryDate_actual', 'current_date') < 0)
                                        & ((F.col('originalOperator') != '')
                                           | (F.col('currentOperator') != '')))))), 'In Service')
