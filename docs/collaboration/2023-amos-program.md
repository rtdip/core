# RTDIP AI Pipeline Generation: Our First AMOS 2023 Collaboration

<center>

<img src="https://raw.githubusercontent.com/rtdip/core/develop/docs/blog/images/agile.svg" width="60%" alt="collaboration" />

</center>

The 2023 AMOS project marked **RTDIP's first collaboration** with the Agile Methods and Open Source initiative. A dedicated student team undertook an ambitious challenge: develop an **artificial intelligence tool that could automatically generate RTDIP data ingestion pipelines** based on high-level user requirements.


## About AMOS and Our Partnership

The **Agile Methods and Open Source (AMOS) project** is a unique initiative bringing together university students from multiple institutions with industry partners to tackle real-world software engineering challenges using agile methodologies. During the 2023 academic year, we welcomed our first AMOS team to explore innovative approaches to RTDIP.

The team's mission was ambitious: create an AI-driven system capable of understanding user requirements and automatically composing them into functional RTDIP pipeline configurations using the platform's rich library of components.

## The AI Pipeline Generation Challenge

### The Vision
The project aimed to lower the barrier to entry for RTDIP users by leveraging AI to:

- **Understand requirements** in natural language

- **Select appropriate components** from the RTDIP component library

- **Configure pipelines** with correct parameters and dependencies

- **Generate valid pipeline definitions** ready for execution

### The Reality: Lessons from AI-Driven Development

While the vision was compelling, the project revealed important lessons about the current state of AI in software engineering:

#### Challenge 1: AI Component Hallucination
One of the most significant challenges was that the AI system would frequently **invent non-existent components** or suggest components that didn't exist in RTDIP. For example:

- Proposing components with names like `OptimizedDeltaMerge` that weren't in the framework

- Suggesting parameters or configuration options that the actual components didn't support

- Creating entirely fictional transformation pipelines that violated RTDIP's architectural patterns

**Impact:** Extensive validation and correction was needed to ensure generated pipelines were feasible.

#### Challenge 2: Component Configuration Errors
When selecting real RTDIP components, the AI would often:

- **Incorrectly format component parameters** - Using wrong data types or structures

- **Miss required fields** - Omitting mandatory configuration options

- **Violate component constraints** - Specifying incompatible input/output combinations

- **Misunderstand dependencies** - Creating pipelines where component outputs didn't match expected inputs

**Impact:** Generated pipelines required significant manual correction before execution.

#### Challenge 3: Architectural Understanding
The AI system struggled to grasp RTDIP's core architectural principles:
- The relationship between source, transformer, and destination components
- Schema evolution and data type compatibility
- Partition strategy requirements for Spark operations
- Error handling and data quality expectations

## Project Outcomes

Despite these challenges, the project provided valuable insights:

### What We Learned
1. **Current AI limitations** in understanding complex software architectures
2. **The importance of structured training data** for specialized domains
3. **How context windows and knowledge cutoffs** limit AI capabilities for rapidly evolving platforms
4. **The value of explicit constraints** and validation in automated code/config generation

### Deliverables
- A proof-of-concept AI pipeline generation system
- Comprehensive documentation of challenges and limitations
- Recommendations for improving AI-assisted pipeline development
- Test cases and validation frameworks for generated pipelines

## Looking Forward: An Interesting Opportunity

The 2023 AMOS project was conducted when foundational models were still relatively young. Over the past few years, significant advances have been made in:

- **Larger, more sophisticated models** with better understanding of code and specifications
- **Fine-tuning and specialized variants** trained on specific domains and languages
- **Retrieval-augmented generation (RAG)** enabling better integration with live documentation
- **Chain-of-thought reasoning** improving logical pipeline composition
- **Code grounding and execution checking** reducing hallucinations

### A Compelling Rerun

This project would be an **excellent candidate for re-execution** given modern AI capabilities:

1. **Better component understanding** - Modern models could leverage RTDIP's expanded documentation
2. **Improved constraint handling** - Current AI systems better understand structured specifications and validation rules
3. **Enhanced reasoning** - Contemporary approaches could better understand component interdependencies
4. **Real-world validation** - Test against years of actual RTDIP pipeline patterns and best practices

We believe that with today's AI tools and methodologies, this project could evolve from a challenging proof-of-concept into a genuinely useful feature that democratizes RTDIP pipeline development.

## The Value of That First Collaboration

Despite the challenges, this 2023 AMOS partnership proved invaluable:

- **Academic-Industry Learning** - Students gained experience with real-world constraints in AI applications
- **Honest Evaluation** - Transparent assessment of AI capabilities and limitations in specialized domains
- **Foundation for Future Work** - Identified specific improvements and architectural changes needed
- **Community Insight** - Shared learnings benefit others exploring AI in data engineering
- **Relationship Building** - Established RTDIP as an open, collaborative partner for innovation

## RTDIP's Commitment to Innovation

RTDIP remains committed to:

- **Exploring cutting-edge approaches** to data pipeline development
- **Partnering with academic institutions** on research and innovation
- **Maintaining transparency** about both successes and challenges
- **Continuously improving** our platform based on lessons learned
- **Supporting future iterations** of innovative projects

---

**Interested in the AMOS project?** Learn more at [amos.cs.fau.de](https://amos.cs.fau.de).  
**Want to collaborate with RTDIP?** Visit our [GitHub repository](https://github.com/rtdip/core) to get involved.