import logging
from config import Config

class GetLogger:
    __slots__ = ('logger', 'config')

    def __call__(self, config: Config = None):
        """Setup and return a logger"""
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        return self.logger 

    def setup_logging(self):
        """Setup logging configs from a config or with default settings."""
        if self.config:
            logging.basicConfig(
                level=getattr(logging, self.config.logging_level.upper(), logging.INFO),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        else:
            #Defaults
            logging.basicConfig(
                filename='db_repo_factory.log',
                filemode='a',
                format='%(asctime)s - %(levelname)s - %(message)s',
                level=logging.INFO
            )
