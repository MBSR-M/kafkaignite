#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import pandas as pd


class Command:
    def __init__(self, successor=None, **args):
        self._successor = successor
        self.args = args

    def execute(self, context):
        """Abstract method to be implemented by subclasses"""
        raise NotImplementedError

    def pass_to_successor(self, context):
        """Pass the context to the next command in the chain, if exists"""
        if self._successor:
            return self._successor.execute(context)
        return context


class LocateFilesCommand(Command):
    def execute(self, context):
        """Locate files based on location and file pattern"""
        path = context.get('location', self.args.get('location'))
        file_pattern = context.get('file_pattern', self.args.get('file_pattern'))
        file_paths = glob.glob(os.path.join(path, file_pattern))
        file_paths.sort()
        context['file_paths'] = file_paths

        return self.pass_to_successor(context)


class LoadDataFramesCommand(Command):
    def execute(self, context):
        """Load data from the located files into pandas DataFrames"""
        file_paths = context.get('file_paths', [])
        processed_dataframes = []

        for file_path in file_paths:
            df = pd.read_excel(file_path)
            processed_dataframes.append(df)

        context['processed_dataframes'] = processed_dataframes
        return self.pass_to_successor(context)


class CombineDataFramesCommand(Command):
    def execute(self, context):
        """Combine all DataFrames into a single DataFrame"""
        processed_dataframes = context.get('processed_dataframes', [])

        if processed_dataframes:
            result_df = pd.concat(processed_dataframes, ignore_index=True)
            context['combined_df'] = result_df
        else:
            context['combined_df'] = pd.DataFrame()
        return self.pass_to_successor(context)


class CombineDataFrames:
    def __init__(self, **args):
        """Initialize with dynamic arguments"""
        self.args = args

    def execute(self):
        command_chain = LocateFilesCommand(
            LoadDataFramesCommand(
                CombineDataFramesCommand(),
                **self.args
            ),
            **self.args
        )
        final_context = command_chain.execute({
            "location": self.args.get('location'),
            "file_pattern": self.args.get('file_pattern')
        })
        return final_context['combined_df']
