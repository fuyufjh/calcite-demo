package me.ericfu.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableInterpreterRule;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.tpch.TpchSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.NoneToBindableConverterRule;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.janino.ClassBodyEvaluator;

import java.io.StringReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class TpchDemo {

    public static void main(String[] args) throws Exception {

        final String sql = TpchQuery.Q9;

        // Build Parser
        SqlParser.Config parserConfig = SqlParser.configBuilder()
            .setLex(Lex.MYSQL_ANSI)
            .setParserFactory(SqlParserImpl.FACTORY)
            .build();
        SqlParser parser = SqlParser.create(sql, parserConfig);

        // Build DataTypeFactory
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        // Build RexBuilder
        RexBuilder rexBuilder = new RexBuilder(typeFactory);

        // Parse
        SqlNode sqlNode = parser.parseStmt();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        // Build Schema
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        TpchSchema tpchSchema = new TpchSchema(0.1, 1, 1, true);
        rootSchema.add("tpch", tpchSchema);

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(rootSchema,
            rootSchema.path("tpch"),
            typeFactory,
            new CalciteConnectionConfigImpl(properties));

        // Build Validator
        SqlValidatorWithHints validator = SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            SqlConformanceEnum.DEFAULT);

        // Validate
        SqlNode validatedSqlNode = validator.validate(sqlNode);


        // ===========================================
        //               Optimization
        // ===========================================

        // Build Volcano Planner
        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.EMPTY_CONTEXT);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.registerAbstractRelationalRules();
        RelOptUtil.registerAbstractRels(planner);


        // Add Transformation Rules
        for (RelOptRule rule : TRANSFORM_RULES) {
            planner.addRule(rule);
        }

        // Add Implementation Rules (Enumerable)
        for (RelOptRule rule : ENUMERABLE_RULES) {
            planner.addRule(rule);
        }

        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // Build SqlToRelConverter
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.configBuilder()
            .withConvertTableAccess(false)
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withExpand(false)
            .build();
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(null, validator, catalogReader, cluster,
            StandardConvertletTable.INSTANCE, converterConfig);

        // Convert SqlNode to RelNode
        RelRoot relRoot = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);

        RelNode rel = relRoot.rel;

        // Stage 1. Subquery Removal
        {
            final HepProgramBuilder hepProgramBuilder = HepProgram.builder();
            for (RelOptRule rule : SUBQUERY_REMOVE_RULES) {
                hepProgramBuilder.addRuleInstance(rule);
            }
            HepProgram hepProgram = hepProgramBuilder.build();
            HepPlanner hepPlanner = new HepPlanner(hepProgram,
                null, true, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(rel);
            rel = hepPlanner.findBestExp();
        }

        // Stage 2. Decorrelate
        {
            final RelBuilder relBuilder =
                RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            rel = RelDecorrelator.decorrelateQuery(rel, relBuilder);
        }

        // Stage 3. Trim Fields (optional)
        {
            final RelBuilder relBuilder =
                RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            rel = new RelFieldTrimmer(null, relBuilder).trim(rel);
        }

        // Stage 4. Volcano Planner
        {
            planner.setRoot(planner.changeTraits(rel, rel.getTraitSet().replace(EnumerableConvention.INSTANCE)));
            rel = planner.findBestExp();
        }

        // Stage 5. Convert Project/Filter to Calc
        {
            final HepProgramBuilder hepProgramBuilder = HepProgram.builder();
            for (RelOptRule rule : CALC_RULES) {
                hepProgramBuilder.addRuleInstance(rule);
            }
            HepProgram hepProgram = hepProgramBuilder.build();
            HepPlanner hepPlanner = new HepPlanner(hepProgram,
                null, true, null, RelOptCostImpl.FACTORY);
            hepPlanner.setRoot(rel);
            rel = hepPlanner.findBestExp();
        }

        System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // ===========================================
        //               Compilation
        // ===========================================

        EnumerableRel enumerableRel = (EnumerableRel) rel;

        // Generate code from Enumerable plan
        EnumerableRelImplementor relImplementor = new EnumerableRelImplementor(rexBuilder, Collections.emptyMap());
        ClassDeclaration classDeclaration = relImplementor.implementRoot(enumerableRel, EnumerableRel.Prefer.ARRAY);

        String code = Expressions.toString(classDeclaration.memberDeclarations, "\n", false);
        Util.debugCode(System.out, code);

        // Compile to executor class
        IClassBodyEvaluator cbe = new ClassBodyEvaluator();
        cbe.setClassName(classDeclaration.name);
        cbe.setExtendedClass(Utilities.class);
        cbe.setImplementedInterfaces(
            enumerableRel.getRowType().getFieldCount() == 1
                ? new Class[] {Bindable.class, Typed.class}
                : new Class[] {ArrayBindable.class});
        cbe.setParentClassLoader(EnumerableInterpretable.class.getClassLoader());
        cbe.setDebuggingInformation(true, true, true);

        // ===========================================
        //                 Execution
        // ===========================================

        DataContext dataContext = new DataContext() {
            @Override
            public SchemaPlus getRootSchema() {
                return rootSchema.plus();
            }

            @Override
            public JavaTypeFactory getTypeFactory() {
                return typeFactory;
            }

            @Override
            public QueryProvider getQueryProvider() {
                return null;
            }

            @Override
            public Object get(String name) {
                return null;
            }
        };

        Bindable<Object[]> bindable = (Bindable<Object[]>) cbe.createInstance(new StringReader(code));
        Enumerable<Object[]> enumerable = bindable.bind(dataContext);

        Iterator<Object[]> iterator = enumerable.iterator();
        while (iterator.hasNext()) {
            StringBuilder sb = new StringBuilder();
            Object[] tuple = iterator.next();
            for (Object datum : tuple) {
                sb.append(datum).append(", ");
            }
            System.out.println(sb.toString());
        }
    }

    private static final List<RelOptRule> TRANSFORM_RULES =
        ImmutableList.of(
            AggregateStarTableRule.INSTANCE,
            AggregateStarTableRule.INSTANCE2,
            TableScanRule.INSTANCE,
            ProjectMergeRule.INSTANCE,
            FilterTableScanRule.INSTANCE,
            ProjectFilterTransposeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            JoinPushExpressionsRule.INSTANCE,
            AggregateExpandDistinctAggregatesRule.INSTANCE,
            AggregateReduceFunctionsRule.INSTANCE,
            FilterAggregateTransposeRule.INSTANCE,
            ProjectWindowTransposeRule.INSTANCE,
            JoinCommuteRule.INSTANCE,
            JoinPushThroughJoinRule.RIGHT,
            JoinPushThroughJoinRule.LEFT,
            SortProjectTransposeRule.INSTANCE,
            SortJoinTransposeRule.INSTANCE,
            SortRemoveConstantKeysRule.INSTANCE,
            SortUnionTransposeRule.INSTANCE
            // Enumerable does not support ProjectableTableScan
            // ProjectTableScanRule.INSTANCE
        );

    public static final List<RelOptRule> ENUMERABLE_RULES =
        ImmutableList.of(
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
            EnumerableRules.ENUMERABLE_CORRELATE_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_COLLECT_RULE,
            EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE,
            EnumerableRules.ENUMERABLE_INTERSECT_RULE,
            EnumerableRules.ENUMERABLE_MINUS_RULE,
            EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
            EnumerableRules.ENUMERABLE_VALUES_RULE,
            EnumerableRules.ENUMERABLE_WINDOW_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE);

    static final List<RelOptRule> SUBQUERY_REMOVE_RULES =
        ImmutableList.of(
            SubQueryRemoveRule.FILTER,
            SubQueryRemoveRule.PROJECT,
            SubQueryRemoveRule.JOIN);

    public static final ImmutableList<RelOptRule> CALC_RULES =
        ImmutableList.of(
            NoneToBindableConverterRule.INSTANCE,
            EnumerableRules.ENUMERABLE_CALC_RULE,
            EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
            CalcMergeRule.INSTANCE,
            FilterCalcMergeRule.INSTANCE,
            ProjectCalcMergeRule.INSTANCE,
            FilterToCalcRule.INSTANCE,
            ProjectToCalcRule.INSTANCE,
            CalcMergeRule.INSTANCE);
}
