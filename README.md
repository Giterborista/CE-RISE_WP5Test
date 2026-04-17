# CERISE-SEE-render

> Render-focused fork: ontology files are intentionally excluded to keep repository lightweight for deployment workflows.

This repository contains the operationalisation of the impact calculation procedure developed in CE-RISE T3.3, as seen in [CE-RISE D3.3](http://ce-rise.eu).

It contains the following:
- A detailed specification of the *data structure* for LCI inputs and LCA outputs.
- A specification of the impact calculation methodology.
- An implementation of this methodology using Brightway2.

## Directory Tree
TBA

## Introduction
### Introduction to LCA 
The aim of environmental impact calculation through Life Cycle Assessment is to quantify the interactions of a **product system** with the environment. The idea is that a product provides a quantifiable function, and that the environmental impacts associated with it fulfilling this function can therefore also be quantified. In principle, different products fulfilling the same function can therefore be reasonably compared.

In essence, LCA follows four stages of calculation:

1. **Goal and scope**:
    
    a. **Goal**: Set the overall context of the study.
    
    b. **Scope**:
    
    - Define the **functional unit** (what is the function the product system provides?) and the **reference flow** (how is this expressed in terms of the output of processes in the product system?).
    - Define the **system boundary** (e.g. cradle-to-grave for a full life cycle perspective).
    - Define the **impact categories** (e.g. potential climate change impacts) for which the environmental impacts of the product system are quantified.
    - List additional assumptions.

2. **Life Cycle Inventory (LCI)**:

    This step quantifies, for the processes in the product system:
    - the inputs and outputs in terms of material, energy, and waste (non-elementary or complex flows);
    - the emissions to air, water, and soil (elementary flows).
    
    In essence, the object of the LCI phase is to completely quantify the inputs and outputs of the product system in terms of elementary flows. The product system is a representation of the value chain, formed as a combination of **activities** (or *unit processes*). These activities are connected by non-elementary flows, and have interactions with the environment in the form of elementary flows. The full LCI is the weighted sum of all interactions of activities in the product system with the environment, scaled to one output of the reference flow.

3. **Life Cycle Impact Assessment (LCIA)**

    The LCIA phase then transforms the total elementary flows required per reference flow of the product system into environmental impacts. This is done using **characterisation factors**, which translate environmental flows (direct environmental interventions) to environmental impact potential.

4. **Interpretation**

    In this step, the results of the study are assessed in terms of how they correspond to the study's goals (as outlined in the Goal and Scope phase). Moreover, conclusions are derived from the analysis.

The implementation outlined here allows for the DPP to complement LCA calculations in two different ways:

1. Communication of activity/unit process data through the DPP to help construct the product system in the LCI phase.
2. Automated impact calculations using this activity/unit process data to calculate impacts associated to product systems with reference flows as defined by these activities.

## Considerations for DPP-LCA implementations
The implementation outlined here allows for the DPP to complement LCA calculations in two different ways:

1. Communication of activity/unit process data through the DPP to help construct the product system in the LCI phase.
2. Automated impact calculations using this activity/unit process data to calculate impacts associated to product systems with reference flows as defined by these activities.

## Ontology for LCA data in the DPP
### Overall structure
![Ontology figure](ontology_structure.png)

The ontology builds on the BONSAI ontology to provide a structure for LCA through DPP. This structure consists of Life Cycle Inventory (LCI) inputs per process in the value chain, detailed as **Activities** (alternatively named unit processes); and Life Cycle Assessment (LCA) outputs for user-defined case studies.

The overall workflow is as follows:
1. The user details their own unit processes as **Activities** in the product's DPP.
2. The inputs and outputs of these processes (**Flows**) are linked with Activities in 
    - the same DPP;
    - other DPPs in the value chain;
    - background information through the BONSAI Environmentally Extended Supply-Use Tables.
3. Based on a user-defined **Functional Unit**, which is specified in terms of the outputs of one or more DPP **Activities**, the system calculates a Life Cycle Assessment result using the BONSAI tables and the Environmental Footprint 3.1 (EF3.1) methods and characterisation factors.
4. These LCA outputs are stored in the DPP as Life Cycle Assessment results.

### LCI inputs
#### Activities
A unit process detailed in the DPP is called an **Activity**. An Activity is a specific process taking place in a specific location in a specific time period (e.g. cartridge manufacturing in Poland in 2025). The ActivityType is a broader classification of what happens in the Activity (e.g. cartridge manufacturing). The ActivityType is therefore structured as a ``skos:Concept``. The ActivityType moreover has a Life Cycle Stage (e.g. manufacturing), and a CPA Classification.

The main object of the Activity data type is to capture its inputs and outputs (e.g. what is required to manufacture a cartridge and what are the direct outputs of this activity). Every Activity has one **DeterminingFlow** (e.g. one new printer cartridge manufactured): the primary object produced or service rendered by the Activity.

#### Flows
Inputs and outputs of activities are named **Flows**. Flows are structured similarly to Activities in the sense that a Flow is a specific quantified instance of a specified type, named a FlowObject (as an Activity is a specified instance with an ActivityType). FlowObjects are classified into three categories:
1. **Primary flows** which are uniquely specified as the DeterminingFlows of other activities in the DPP or in other linked DPPs.
2. **Secondary flows** which are non-elementary (see below) inputs or outputs which are not DeterminingFlows of other DPP activities. These flows are linked to background BONSAI categories.
3. **Elementary flows** which are direct interactions of the unit process with the environment (e.g. direct CO2 emissions to air, or primary resource use). These are classified as EF3.1 elementary flows.

Our ontology provides:
1. A complete mapping of all secondary FlowObjects available in BONSAI, with available units.
2. A complete mapping of all EF3.1 elementary flows, with preferred units and EF3.1 UUIDs.

### LCA Results Output
#### Functional Unit and Reference Flow
A user wishing to produce an LCA result through the system should specify the functional unit of the study under PEF specifications. The most important here is the **Reference Flow**, where the functional unit (e.g. "printing 1000 A4 pages at 5% ISO/IEC 19752 standard page coverage") is translated to the DeterminingFlow of one or more DPP activities (e.g. "1 cartridge use" or "1 new printer catridge"). This is the basis for the system's calculation of impacts associated to this output.

Next to this, the functional unit should be specified according to PEF guidelines, answering the questions *what?*, *how much?*, *how well?*, and *how long?*. Example (European Commission. (2021). Commission Recommendation of 16.12.2021 on the use of the Environmental Footprint methods, p. 28):
> Define the FU of decorative paint: the functional unit is to protect and decorate 1 m2 of substrate for 50 years at a specified quality level (minimum 98% opacity).
> - **What**: Provide decoration and protection of a substrate,
> - **How much**: coverage of 1 m2 of substrate,
> - **How well**: with a minimum - 98% opacity
> - **How long**: for 50 years (life time of the building)
> - **Reference flow**: amount of product needed to fulfil the defined function, to be measured in kg of paint.

These data points are included as free text fields (``rdfs:Literal``).

#### LCAResult
Based on the reference flow specification, the impacts are calculated in the following manner:
- the inputs and outputs of reference flow activities are connected to the activities for which they are a determining flow;
- these activities' inputs and outputs are similarly connected to other activities, up to the point where all inputs and outputs required are:
    - elementary flows, or
    - BONSAI secondary flows.

The impact calculation is implemented through Brightway2 and is detailed in the ``proof_of_concept`` folder. This process takes the specified activities, connects these to the relevant BONSAI categories and environmental flows, and produces a Life Cycle Assessment result, which is split per Life Cycle Stage. This LCAResult therefore has the following data attached:'
1. An **ImpactValue** (e.g. 3 kg CO2-eq., GWP100), which is detailed as an ``om:Measure``, for a specific unit specified in the ontology.
2. An **ImpactCategory**, detailing the impact category this result was produced for (e.g. EF3.1 climate change).
3. An **LCStage**, detailing the life cycle stage these impacts are produced by within the system (e.g. manufacturing). The sum of all life cycle stage impacts is the full product's impacts for that impact category.
4. An **ImpactAssessmentMethod**: one of the EF3.1 impact assessment methods defined for the impact category.
5. A **system boundary** detailing whether the result was produced for cradle-to-grave or for cradle-to-cradle (for intermediate products).
6. The **SecondaryDataSource** detailing the database used for secondary flows (in our case this is a version of the BONSAI table).