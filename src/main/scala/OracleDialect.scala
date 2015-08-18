import java.sql.Types

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{LongType, DataType, MetadataBuilder}


case object OracleDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.NUMERIC && typeName == "NUMBER" && size == 0) Some(LongType) else None
  }
}
