# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Base interfaces for RTDIP visualization components.

This module defines abstract base classes for visualization components,
ensuring consistent APIs across both Matplotlib and Plotly implementations.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Union

from .._pipeline_utils.models import Libraries, SystemType


class VisualizationBaseInterface(ABC):
    """
    Abstract base interface for all visualization components.

    All visualization classes must implement this interface to ensure
    consistent behavior across different backends (Matplotlib, Plotly).

    Methods:
        system_type: Returns the system type (PYTHON)
        libraries: Returns required libraries
        settings: Returns component settings
        plot: Generate the visualization
        save: Save the visualization to file
    """

    @staticmethod
    def system_type() -> SystemType:
        """
        Returns the system type for this component.

        Returns:
            SystemType: Always returns SystemType.PYTHON for visualization components.
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries() -> Libraries:
        """
        Returns the required libraries for this component.

        Returns:
            Libraries: Libraries instance (empty by default, subclasses may override).
        """
        return Libraries()

    @staticmethod
    def settings() -> dict:
        """
        Returns component settings.

        Returns:
            dict: Empty dictionary by default.
        """
        return {}

    @abstractmethod
    def plot(self) -> Any:
        """
        Generate the visualization.

        Returns:
            The figure object (matplotlib.figure.Figure or plotly.graph_objects.Figure)
        """
        pass

    @abstractmethod
    def save(
        self,
        filepath: Union[str, Path],
        **kwargs,
    ) -> Path:
        """
        Save the visualization to file.

        Args:
            filepath: Output file path
            **kwargs: Additional save options (dpi, format, etc.)

        Returns:
            Path: The path to the saved file
        """
        pass


class MatplotlibVisualizationInterface(VisualizationBaseInterface):
    """
    Interface for Matplotlib-based visualization components.

    Extends the base interface with Matplotlib-specific functionality.
    """

    @staticmethod
    def libraries() -> Libraries:
        """
        Returns required libraries for Matplotlib visualizations.

        Returns:
            Libraries: Libraries instance with matplotlib, seaborn dependencies.
        """
        libraries = Libraries()
        libraries.add_pypi_library("matplotlib>=3.3.0")
        libraries.add_pypi_library("seaborn>=0.11.0")
        return libraries


class PlotlyVisualizationInterface(VisualizationBaseInterface):
    """
    Interface for Plotly-based visualization components.

    Extends the base interface with Plotly-specific functionality.
    """

    @staticmethod
    def libraries() -> Libraries:
        """
        Returns required libraries for Plotly visualizations.

        Returns:
            Libraries: Libraries instance with plotly dependencies.
        """
        libraries = Libraries()
        libraries.add_pypi_library("plotly>=5.0.0")
        libraries.add_pypi_library("kaleido>=0.2.0")
        return libraries

    def save_html(self, filepath: Union[str, Path]) -> Path:
        """
        Save the visualization as an interactive HTML file.

        Args:
            filepath: Output file path

        Returns:
            Path: The path to the saved HTML file
        """
        return self.save(filepath, format="html")

    def save_png(self, filepath: Union[str, Path], **kwargs) -> Path:
        """
        Save the visualization as a static PNG image.

        Args:
            filepath: Output file path
            **kwargs: Additional options (width, height, scale)

        Returns:
            Path: The path to the saved PNG file
        """
        return self.save(filepath, format="png", **kwargs)
