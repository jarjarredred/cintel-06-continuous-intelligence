# Continuous Intelligence

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## Custom Project

### Dataset
system_metric_case.csv consists of raw system metrics of computer system.

### Signals
Error rate is errors/requests and represents the reliability of the system. Latency is total_latency_ms/requests and represents the efficiency of a single request. The Rolling Average is the drift signal and consists of the rolling_mean of latency cost over a window, it represents momentum and direction.

### Experiments
Instead of doing a simple threshold check, the new problem to address could be a trend analsys and assessing resource efficiency. Calculated a cost-per-request signal and used a rolling average to detect if system is drifting towards "instability".

### Results
Using the same metric data, the output file identified some periods of time that indicated the system was drifting towards instability and some periods of time when it was stable.

### Interpretation
A user can readily assess whether the system is in danger without during further calculations.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Glossary** - project terms and concepts

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)
