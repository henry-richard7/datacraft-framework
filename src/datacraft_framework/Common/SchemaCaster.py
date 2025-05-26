import polars
from Models.schema import CtlColumnMetadata


class SchemaCaster:
    """
    A class to cast columns of a Polars DataFrame to specified data types based on metadata.

    This class takes a DataFrame and a list of column metadata objects that define the desired
    data type (and optionally date format) for each column. It applies casting operations to
    transform the DataFrame schema accordingly.
    """

    def __init__(self, df: polars.DataFrame, column_metadata: list[CtlColumnMetadata]):
        """
        Initialize the SchemaCaster with a DataFrame and column metadata.

        Args:
            df (polars.DataFrame): The DataFrame whose columns will be cast.
            column_metadata (List[CtlColumnMetadata]): A list of metadata objects where each object
                contains at least `column_name` and `column_data_type`. Optionally includes
                `column_date_format` if the type is 'date'.
        """
        self.df = df
        self.column_metadata = column_metadata

    def start(self) -> polars.DataFrame:
        """
        Apply schema casting operations to the DataFrame based on metadata.

        Iterates through the column metadata and performs the following casts:
            - `"integer"` → `Int32`
            - `"float"` → `Float32`
            - `"double"` → `Float64`
            - `"long"` → `Int64`
            - `"string"` → `String`
            - `"boolean"` → `Boolean`
            - `"date"` → Converts string to `Date` using the specified format

        Returns:
            polars.DataFrame: A new DataFrame with columns cast to the specified data types.

        Raises:
            polars.exceptions.PolarsException: If casting fails due to incompatible data.
        """
        for metadata in self.column_metadata:
            if metadata.column_data_type == "integer":
                self.df = self.df.with_columns(
                    polars.col(metadata.column_name).cast(polars.Int32)
                )
            elif metadata.column_data_type == "float":
                self.df = self.df.with_columns(
                    polars.col(metadata.column_name).cast(polars.Float32)
                )
            elif metadata.column_data_type == "double":
                self.df = self.df.with_columns(
                    polars.col(metadata.column_name).cast(polars.Float64)
                )
            elif metadata.column_data_type == "long":
                self.df = self.df.with_columns(
                    polars.col(metadata.column_name).cast(polars.Int64)
                )
            elif metadata.column_data_type == "string":
                self.df = self.df.with_columns(
                    polars.col(metadata.column_name).cast(polars.String)
                )
            elif metadata.column_data_type == "boolean":
                self.df = self.df.with_columns(
                    polars.col(metadata.column_name).cast(polars.Boolean)
                )

            elif metadata.column_data_type == "date":
                self.df = self.df.with_columns(
                    polars.col(metadata.column_name).str.to_date(
                        format=metadata.column_date_format
                    )
                )
        return self.df
