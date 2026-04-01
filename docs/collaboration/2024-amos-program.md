# RTDIP Data Quality: An AMOS 2024 Collaboration

<center>

<img src="https://raw.githubusercontent.com/rtdip/core/develop/docs/blog/images/agile.svg" width="60%" alt="collaboration" />

</center>

The Agile Methods and Open Source (AMOS) project represents a unique collaborative opportunity where students from TU Berlin and FAU Erlangen-Nürnberg worked with the RTDIP community to explore data quality enhancements for real-time data integration.

## About AMOS

The AMOS project is an open-source initiative that brings together academic institutions and industry partners to tackle real-world software engineering challenges using agile methodologies. Students participate in SCRUM teams, gaining hands-on experience with modern development practices while contributing to meaningful open-source projects.

During the 2024-2025 academic period, the AMOS team chose to build upon RTDIP's ingestion pipeline framework as a foundation for their work on **data quality measures and enhancements**.

## The Data Quality Initiative

The AMOS team developed a comprehensive set of data quality components including:

- **Data Validation & Schema Alignment** - InputValidator for schema compliance and data type verification
- **Data Cleansing** - Duplicate detection, flatline filters, range validation, and anomaly detection
- **Missing Value Imputation** - Intelligent gap-filling using spline interpolation and historical patterns
- **Normalization** - Mean normalization, min-max scaling, z-score standardization with denormalization support
- **Data Monitoring** - Flatline detection and missing data identification for continuous quality tracking
- **Time Series Forecasting** - ARIMA and AutoARIMA models for predictive analytics

These components were built as modular, extensible pipeline steps designed to help data scientists and engineers with data integration, cleaning, and preparation workflows.

## Important Note: Component Status

**Please be aware:** The data quality and forecasting components developed during the AMOS project are no longer integrated into the core RTDIP SDK as of v0.14.3. This decision was made to keep RTDIP focused on its core time-series data ingestion and querying capabilities.

If you're interested in the work completed by the AMOS team and want to use these components, please refer to the **AMOS project repository** where they maintain this work as a separate, specialized extension.

## Learning from the Collaboration

The AMOS project is an excellent example of how open-source communities can collaborate with academic institutions to:

- Foster practical software engineering education
- Explore new ideas and approaches in a real-world context  
- Maintain focused, maintainable project scope
- Enable specialized extensions while keeping core projects lean

The AMOS team's experience with agile methodologies, SCRUM practices, and collaborative development demonstrates valuable lessons for both industry and academia.

## Connecting with AMOS

For more information about the AMOS project, its data quality components, and current status:

- **AMOS Project Website:** [amos.cs.fau.de](https://amos.cs.fau.de)
- **Project Repository:** Check the AMOS GitHub organization for the data quality extension repository
- **Contact:** Reach out to the AMOS team or participating universities for more details

## RTDIP's Commitment

While these specific data quality components are no longer part of core RTDIP, we remain committed to:

- **Great foundations** for building custom extensions and specializations
- **Clear architectural patterns** that enable modular pipeline development
- **Strong community collaboration** with academic and industry partners

If you're building specialized data quality solutions, RTDIP's core pipeline framework provides an excellent foundation for your project.

**Interested in collaborating with RTDIP?** We welcome partnerships and contributions. Visit our [GitHub repository](https://github.com/rtdip/core) to learn how to get involved.
