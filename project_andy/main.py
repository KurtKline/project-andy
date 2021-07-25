from pathlib import Path

from loguru_setup import get_logger
from project_file_processor import ProjectFileProcessor

logger = get_logger(Path(__file__).parent.parent / 'logs/run.log')


def main():
    projectFileProcessor = ProjectFileProcessor()
    projectFileProcessor.process_file('20210724')
    projectFileProcessor.process_file('20210725')


if __name__ == '__main__':
    main()
