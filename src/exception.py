import sys


class M5Exception(Exception):

    def __init__(self, error_message: str, error_detail: sys):
        super().__init__(error_message)

        _, _, exc_tb = sys.exc_info()

        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno

        self.error_message = (
            f"Error in {file_name}, "
            f"line {line_number}: {error_message}"
        )

    def __str__(self):
        return self.error_message