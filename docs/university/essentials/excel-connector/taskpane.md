# Exercise: Exploring the Taskpane

The taskpane is the UI that guides you through and manages queries to the RTDIP API via your Excel sheet. All the API query types are supported, and can be accessed via the dropdown menu.

>
When you click run on the task pane, it inserts a formula with the specified parameters into a cell. We'll dive deeper into these custom functions in the next exercise.
>

**Try these exercises to get familiar with the taskpane:**

1. Run a `Raw` query by filling in the parameters as you did in the API lesson.
2. Run an `Interpolate` query in the same way.
3. Try the shorthand parameters, for example rather than `todays date` you can do `*`, and `yesterdays date` you can do `*-1d`.
4. Search for a different tag with the tag explorer ![tagsearch](assets/tagsearch-icon.png){width=30px} and add one to your query.
5. Explore the dashboard ![tagsearch](assets/dashboard-icon.png){width=30px} and `edit`, `delete` or `refresh` one of your queries.


## Additional Task

1. Swtich to the `SQL` form and write a SQL query (note: these do not have to be timeseries tables)
2. Open up the settings and change the look of the headers (or even turn them off).
3. Look at the `Advanced Parameters` and try changing them (**do not change the refresh interval, we will do this in the final exercise**).


Onto the next section: exploring the functions directly!

<br></br>
[← Previous](./getting-started.md){ .curved-button }
[Next →](./functions.md){ .curved-button }

## Course Progress
-   [X] Overview
-   [X] SDK
-   [X] Power BI
-   [X] APIs
-   [ ] Excel Connector
    *   [X] Overview
    *   [X] Getting Started
    *   [X] Exercise: Exploring the Taskpane
    *   [ ] Exercise: Exploring the Functions
    *   [ ] Exercise: Creating a Simple Dashboard with Live Data