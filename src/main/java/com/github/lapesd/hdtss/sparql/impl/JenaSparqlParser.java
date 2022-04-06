package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.*;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.EmptySparqlException;
import com.github.lapesd.hdtss.sparql.FeatureNotSupportedException;
import com.github.lapesd.hdtss.sparql.SparqlParser;
import com.github.lapesd.hdtss.sparql.SparqlSyntaxException;
import com.github.lapesd.hdtss.utils.JenaUtils;
import jakarta.inject.Singleton;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_Exists;
import org.apache.jena.sparql.expr.E_NotExists;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.syntax.*;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.github.lapesd.hdtss.utils.JenaUtils.fromBinding;
import static com.github.lapesd.hdtss.utils.JenaUtils.toSPARQL;
import static java.util.stream.Collectors.toList;

@Singleton
public class JenaSparqlParser implements SparqlParser {
    public JenaSparqlParser() {
        JenaUtils.init();
    }

    @Override
    public @NonNull Op parse(@NonNull CharSequence sparql) throws FeatureNotSupportedException {
        if (sparql.isEmpty() || (sparql.length() < 16 && sparql.toString().trim().isEmpty()))
            throw new EmptySparqlException();
        String queryString = sparql.toString();
        Query query;
        try {
            query = QueryFactory.create(queryString);
        } catch (QueryException e) {
            throw new SparqlSyntaxException(e.getMessage(), queryString);
        }
        return fromJena(queryString, query);
    }

    private @NonNull Op fromJena(@NonNull String queryString, @NonNull Query query) {
        checkType(query, queryString);
        Op root = convertBody(query.getQueryPattern(), queryString);
        List<String> vars = query.getResultVars();
        if (query.hasValues())
            root = new Values(convertValues(query), root);
        if (!vars.equals(root.varNames()) && !query.isAskType())
            root = new Project(vars, root);
        if (query.isDistinct())
            root = new Distinct(root);
        if (query.hasLimit())
            root = new Limit(query.getLimit(), root);
        if (query.hasOffset())
            root = new Offset(query.getOffset(), root);
        if (query.isAskType())
            root = new Ask(root);
        return root;
    }

    private @NonNull Op convertBody(@NonNull Element rootElement, @NonNull String queryString) {
        Op[] op = {IdentityNode.INSTANCE};
        rootElement.visit(new ElementVisitorBase() {
            @Override public void visit(ElementTriplesBlock el) {
                List<Op> tps = new ArrayList<>();
                for (Triple triple : el.getPattern()) tps.add(JenaUtils.fromTriple(triple));
                op[0] = new Join(tps);
            }

            @Override public void visit(ElementUnion el) {
                List<Op> ops = new ArrayList<>(el.getElements().size());
                for (Element child : el.getElements()) {
                    op[0] = IdentityNode.INSTANCE;
                    child.visit(this);
                    ops.add(op[0]);
                }
                op[0] = Union.of(ops);
            }

            @Override public void visit(ElementFilter el) {
                op[0] = Filter.withFilters(op[0], toSPARQL(el.getExpr()));
            }

            @Override public void visit(ElementAssign el) {
                String name = el.getVar().getName(), expr = toSPARQL(el.getExpr());
                op[0] = Assign.withAssignments(op[0], Map.of(name, expr));
            }

            @Override public void visit(ElementBind el) {
                String name = el.getVar().getName(), expr = toSPARQL(el.getExpr());
                op[0] = Assign.withAssignments(op[0], Map.of(name, expr));
                super.visit(el);
            }

            @Override public void visit(ElementData el) {
                List<String> names = el.getVars().stream().map(Var::getVarName).toList();
                var list = el.getRows().stream().map(b -> fromBinding(names, b)).toList();
                op[0] =  new Values(new BatchQuerySolutions(names, list), op[0]);
            }

            @Override public void visit(ElementDataset el) {
                throw new FeatureNotSupportedException("SELECT FROM", queryString);
            }

            @Override public void visit(ElementOptional el) {
                Op left = op[0];
                el.getOptionalElement().visit(this);
                op[0] = new LeftJoin(left, op[0]);
            }

            private static final Set<Class<?>> END_GROUP_CLASSES = Set.of(
                    ElementFilter.class, ElementExists.class,
                    ElementNotExists.class, ElementMinus.class, ElementOptional.class
            );

            @Override public void visit(ElementGroup el) {
                List<@NonNull Op> list = new ArrayList<>(el.getElements().size());
                for (Element child : el.getElements()) {
                    if (!END_GROUP_CLASSES.contains(child.getClass())) {
                        op[0] = IdentityNode.INSTANCE;
                        child.visit(this);
                        list.add(op[0]);
                    }
                }
                op[0] = Join.of(list);
                for (Element child : el.getElements()) {
                    if (child instanceof ElementOptional)
                        child.visit(this);
                }
                List<Element> nonFilter = new ArrayList<>();
                for (Element child : el.getElements()) {
                    if (child instanceof ElementFilter f) {
                        Expr expr = f.getExpr();
                        if (expr instanceof E_NotExists ne) {
                            nonFilter.add(new ElementNotExists(ne.getElement()));
                        } else if (expr instanceof E_Exists ex) {
                            nonFilter.add(new ElementExists(ex.getElement()));
                        } else {
                            child.visit(this);
                        }
                    } else if (END_GROUP_CLASSES.contains(child.getClass())) {
                        nonFilter.add(child);
                    }
                }
                for (Element child : nonFilter) {
                    Op outer = op[0];
                    if      (child instanceof ElementExists   ) child.visit(this);
                    else if (child instanceof ElementNotExists) child.visit(this);
                    else if (child instanceof ElementMinus    ) child.visit(this);
                }
            }

            @Override public void visit(ElementNamedGraph el) {
                throw new FeatureNotSupportedException("Named GRAPH blocks", queryString);
            }

            @Override public void visit(ElementExists el) {
                Op outer = op[0];
                el.getElement().visit(this);
                op[0] = Exists.create(outer, op[0]);
            }

            @Override public void visit(ElementNotExists el) {
                Op outer = op[0];
                el.getElement().visit(this);
                op[0] = Exists.not(outer, op[0]);
            }

            @Override public void visit(ElementMinus el) {
                Op outer = op[0];
                el.getMinusElement().visit(this);
                op[0] = new Minus(outer, op[0]);
            }

            @Override public void visit(ElementService el) {
                throw new FeatureNotSupportedException("SERVICE", queryString);
            }

            @Override public void visit(ElementSubQuery el) {
                op[0] = fromJena(queryString, el.getQuery());
            }

            @Override public void visit(ElementPathBlock el) {
                List<Op> tps = new ArrayList<>(el.getPattern().size());
                for (TriplePath path : el.getPattern()) {
                    if (path.isTriple())
                        tps.add(JenaUtils.fromTriple(path.asTriple()));
                    else
                        throw new FeatureNotSupportedException("Path expressions", queryString);
                }
                op[0] = new Join(tps);
            }
        });
        return op[0];
    }

    private @NonNull QuerySolutions convertValues(@NonNull Query query) {
        var vars = query.getValuesVariables();
        var rows = query.getValuesData().stream().map(b -> {
            Term[] terms = new Term[vars.size()];
            for (int i = 0; i < terms.length; i++)
                terms[i] = JenaUtils.fromNode(b.get(vars.get(i)));
            return terms;
        }).collect(toList());
        return new BatchQuerySolutions(vars.stream().map(Var::getName).collect(toList()), rows);
    }


    private void checkType(@NonNull Query query, @NonNull String queryString) {
        if (query.isConstructType())
            throw new FeatureNotSupportedException("CONSTRUCT", queryString);
        if (query.isDescribeType())
            throw new FeatureNotSupportedException("DESCRIBE", queryString);
    }
}
