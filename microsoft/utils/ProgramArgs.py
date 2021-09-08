import sys
import os
import argparse

class ProgramArguments:
    def __init__(self, args:list):
        self.parser = argparse.ArgumentParser(description='A tutorial of argparse!')
        self.parser.add_argument("-rebase",action="store_true",help="Rebase stored records, industry required")
        self.parser.add_argument("-validate", action="store_true", help="Validate files for an industry, industry required")
        self.parser.add_argument("-ingest", action="store_true", help="Import files to storage, -settings required")
        self.parser.add_argument("-industry", required=False, default=None, type=str, help="Industry required for -rebase and -validate")
        self.parser.add_argument("-settings", required=False, default=None, type=str, help="Json file (exampleinput.json) required for import to table.")
        
        self.arguments = self.parser.parse_args(args)

    @property
    def rebase(self):
        return self.arguments.rebase

    @property
    def validate(self):
        return self.arguments.validate

    @property
    def industry(self):
        return self.arguments.industry

    @property
    def settings(self):
        return self.arguments.settings

    @property
    def ingest(self):
        return self.arguments.ingest

    def validate_args(self):
        count = 0
        if self.arguments.rebase:
            count += 1
        if self.arguments.validate: 
            count += 1
        if self.arguments.ingest:
            count += 1

        if count != 1:
            raise Exception("You must identify one: -rebase, -validate, -ingest")

        if (self.arguments.rebase or self.arguments.validate) and not self.arguments.industry:
            raise Exception("-industry required for -rebase and -validate")

        if self.arguments.ingest:
            if not self.arguments.settings:
                raise Exception("-settings required for -ingest")
            if not os.path.exists(self.arguments.settings):
                raise Exception("-settings does not point to valid file")
