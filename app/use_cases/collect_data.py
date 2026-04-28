import pandas as pd
import io

class DataCollectorUseCase:
    def from_json(self, json_data: list) -> bytes:
        df = pd.DataFrame(json_data)
        return self._to_csv_bytes(df)

    def from_manual_entry(self, raw_text: str) -> bytes:
        # Logique pour transformer du texte saisi en DataFrame
        df = pd.read_csv(io.StringIO(raw_text))
        return self._to_csv_bytes(df)

    def _to_csv_bytes(self, df: pd.DataFrame) -> bytes:
        output = io.BytesIO()
        df.to_csv(output, index=False)
        return output.getvalue()