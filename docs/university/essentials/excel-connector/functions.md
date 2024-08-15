# Exercise: Exploring the Functions

As you will have seen in the previous execise, the Excel Add-in gets data from the RTDIP API with custom functions. In fact, each api route has it's own custom function in Excel.

**Try these exercises to get familiar with the functions:**

1. Write a function directly by referencing parameter values to cells. First, place your parameters in cells (e.g. In cell `B2` put your tagname). Then, in the cell where you want your data, write `=RTDIP.` and you will see the various functions available. Excel will hint which parameters go where.
2. Refactor a formula in your sheet from a previous exercise and change the inputs to reference cells.

A function may look like: 
`=RTDIP.RAW("apiUrl", "region", etc...)`

## Additional Task

1. Try removing optional parameters. These are shown with square brackets around them, for example `[includeBadData]`. If not input, the defaults will be input behind the scenes.

Let's continue to the final section:

<br></br>
[← Previous](./taskpane.md){ .curved-button }
[Next →](./dashboard.md){ .curved-button }

## Course Progress
-   [X] Overview
-   [X] SDK
-   [X] Power BI
-   [X] APIs
-   [ ] Excel Connector
    *   [X] Overview
    *   [X] Getting Started
    *   [X] Exercise: Exploring the Taskpane
    *   [X] Exercise: Exploring the Functions
    *   [ ] Exercise: Creating a Simple Dashboard with Live Data