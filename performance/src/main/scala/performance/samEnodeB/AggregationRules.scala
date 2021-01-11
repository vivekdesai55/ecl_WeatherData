package performance.samEnodeB

object AggregationRules {
  
  //val SINR_Distribution = List("SINR_Distribution","NE_VERSION","NE_ID","NE_NAME","cNum","EarfcnDl","datetime","circle")
  //val Agg_Rule_SINR_Distribution = Map("SinrDistULWbPreComp_Bin0_count"-> "SUM", "SinrDistULWbPreComp_Bin1_count"-> "MAX")
  
  val Agg_Rule_SINR_Distribution = Map(
      "SinrDistULWbPreComp_Bin0_count" -> "SUM",
      "SinrDistULWbPreComp_Bin1_count" -> "SUM",
      "SinrDistULWbPreComp_Bin2_count" -> "SUM",
      "SinrDistULWbPreComp_Bin3_count" -> "SUM",
      "SinrDistULWbPreComp_Bin4_count" -> "SUM",
      "SinrDistULWbPreComp_Bin5_count" -> "SUM",
      "SinrDistULWbPreComp_Bin6_count" -> "SUM",
      "SinrDistULWbPreComp_Bin7_count" -> "SUM",
      "SinrDistULWbPreComp_Bin8_count" -> "SUM",
      "SinrDistULWbPreComp_Bin9_count" -> "SUM",
      "SinrDistULWbPreComp_Bin10_count" -> "SUM",
      "SinrDistULWbPreComp_Bin11_count" -> "SUM",
      "SinrDistULWbPreComp_Bin12_count" -> "SUM",
      "SinrDistULWbPreComp_Bin13_count" -> "SUM",
      "SinrDistULWbPreComp_Bin14_count" -> "SUM",
      "SinrDistULWbPreComp_Bin15_count" -> "SUM",
      "SinrDistULWbPreComp_Bin16_count" -> "SUM",
      "SinrDistULWbPreComp_Bin17_count" -> "SUM",
      "SinrDistULWbPreComp_Bin18_count" -> "SUM",
      "SinrDistULWbPreComp_Bin19_count" -> "SUM",
      "SinrDistULWbPostComp_Bin0_count" -> "SUM",
      "SinrDistULWbPostComp_Bin1_count" -> "SUM",
      "SinrDistULWbPostComp_Bin2_count" -> "SUM",
      "SinrDistULWbPostComp_Bin3_count" -> "SUM",
      "SinrDistULWbPostComp_Bin4_count" -> "SUM",
      "SinrDistULWbPostComp_Bin5_count" -> "SUM",
      "SinrDistULWbPostComp_Bin6_count" -> "SUM",
      "SinrDistULWbPostComp_Bin7_count" -> "SUM",
      "SinrDistULWbPostComp_Bin8_count" -> "SUM",
      "SinrDistULWbPostComp_Bin9_count" -> "SUM",
      "SinrDistULWbPostComp_Bin10_count" -> "SUM",
      "SinrDistULWbPostComp_Bin11_count" -> "SUM",
      "SinrDistULWbPostComp_Bin12_count" -> "SUM",
      "SinrDistULWbPostComp_Bin13_count" -> "SUM",
      "SinrDistULWbPostComp_Bin14_count" -> "SUM",
      "SinrDistULWbPostComp_Bin15_count" -> "SUM",
      "SinrDistULWbPostComp_Bin16_count" -> "SUM",
      "SinrDistULWbPostComp_Bin17_count" -> "SUM",
      "SinrDistULWbPostComp_Bin18_count" -> "SUM",
      "SinrDistULWbPostComp_Bin19_count" -> "SUM"   
 )
  
  
  
  
  val colDict = Map("SINR_Distribution" -> (List("SINR_Distribution","NE_VERSION","NE_ID","NE_NAME","cNum","EarfcnDl","datetime","circle"), Agg_Rule_SINR_Distribution))
}