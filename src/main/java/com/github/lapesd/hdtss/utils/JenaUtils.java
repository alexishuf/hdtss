package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.RDFS;
import com.github.lapesd.hdtss.vocab.XSD;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprNone;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.vocabulary.OWL2;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.jena.graph.NodeFactory.*;

public class JenaUtils {
    private static final @NonNull Set<String> BI_OPS = Set.of("<",  "<=", "=", "==", "!=",
                                                              ">=", ">",  "+", "-",  "*",
                                                              "/",  "&&", "||");
    private static final @NonNull Set<String> UN_OPS = Set.of("!", "-", "+");
    private static final @NonNull SerializationContext SER_CTX = new SerializationContext();
    private static final @NonNull Pattern FILTER_RX = Pattern.compile("(?i)\\s*FILTER\\((.*)\\)\\s*");
    private static final @NonNull String FILTER_BASE_SPARQL =
            "PREFIX xsd: <"+XSD.NS+">\n"+
            "PREFIX rdf: <"+RDF.NS+">\n" +
            "PREFIX rdfs: <"+RDFS.NS+">\n" +
            "PREFIX owl: <"+OWL2.NS+">\n" +
            "SELECT * WHERE {\n";
    private static final @NonNull Pattern UNESCAPED_D_QUOTE = Pattern.compile("([^\\\\]|^)\"");

    public static Node toNode(Term term) {
        if (term == null)
            return null;
        String str = term.content().toString(), lang = term.langAsString();
        return switch (term.type()) {
            case VAR -> createVariable(str);
            case URI -> createURI(str);
            case LITERAL -> lang != null ? createLiteral(str, lang)
                                         : createLiteral(str, toDatatype(term.datatype()));
            case BLANK -> str.isEmpty() ? createBlankNode() : createBlankNode(str);
        };
    }

    public static @NonNull RDFDatatype toDatatype(@Nullable CharSequence uri) {
        if (uri == null)
            uri = XSD.string;
        return TypeMapper.getInstance().getSafeTypeByName(uri.toString());
    }

    public static Term fromNode(Node node) {
        CharSequence sparql;
        if (node == null) {
            return null;
        } else if (node.isURI()) {
            sparql = "<"+node.getURI()+">";
        } else if (node.isBlank()) {
            sparql = "_:"+node.getBlankNodeLabel();
        } else if (node.isVariable()) {
            sparql = node.toString();
        } else if (node.isLiteral()) {
            String lang = node.getLiteralLanguage();
            String dt = node.getLiteralDatatypeURI();
            String lexicalForm = node.getLiteralLexicalForm();
            StringBuilder sb = new StringBuilder(lexicalForm.length()+50);
            sb.append('"');
            Matcher m = UNESCAPED_D_QUOTE.matcher(lexicalForm);
            while (m.find())
                m.appendReplacement(sb, "$1\\\"");
            m.appendTail(sb);
            sb.append('"');
            if (lang != null && !lang.isEmpty())
                sb.append('@').append(lang);
            else if (dt != null)
                sb.append("^^<").append(dt).append('>');
            sparql = sb.toString();
        } else {
            String msg = "Cannot convert " + node + " of " + node.getClass() + " to a Term object";
            throw new IllegalArgumentException(msg);
        }
        return new Term(sparql);
    }

    public static @NonNull TriplePattern fromTriple(@NonNull Triple triple) {
        return new TriplePattern(fromNode(triple.getSubject()),
                                 fromNode(triple.getPredicate()),
                                 fromNode(triple.getObject()));
    }

    public static @NonNull Triple toTriple(@NonNull TriplePattern tp) {
        return new Triple(toNode(tp.subject()), toNode(tp.predicate()), toNode(tp.object()));
    }

    public static @NonNull RowJenaBinding toBinding(@NonNull SolutionRow row,
                                                    @NonNull List<String> varNames) {
        RowJenaBinding binding = new RowJenaBinding(varNames);
        binding.setRow(row);
        return binding;
    }

    public static @NonNull Term[] fromBindingToArray(@NonNull List<String> varNames,
                                                     @NonNull Binding binding) {
        Term[] terms = new Term[varNames.size()];
        for (int i = 0; i < terms.length; i++)
            terms[i] = fromNode(binding.get(Var.alloc(varNames.get(i))));
        return terms;
    }
    public static @NonNull SolutionRow fromBinding(@NonNull List<String> varNames,
                                                   @NonNull Binding binding) {
        return new SolutionRow(fromBindingToArray(varNames, binding));
    }

    public static @NonNull Expr parseFilter(@NonNull String filter) {
        return parseFilters(List.of(filter)).get(0) ;
    }

    public static @NonNull List<@NonNull Expr> parseFilters(@NonNull List<String> filters) {
        var sb = new StringBuilder(FILTER_BASE_SPARQL.length()+64).append(FILTER_BASE_SPARQL);
        for (String s : filters)
            sb.append("FILTER(").append(FILTER_RX.matcher(s).replaceAll("$1")).append(')');
        sb.append('}');
        Query query = QueryFactory.create(sb.toString());
        List<@NonNull Expr> parsedFilters = new ArrayList<>(filters.size());
        query.getQueryPattern().visit(new ElementVisitorBase() {
            @Override public void visit(ElementFilter el) {
                parsedFilters.add(el.getExpr());
            }
            @Override public void visit(ElementGroup el) {
                for (Element e : el.getElements()) e.visit(this);
            }
        });
        return parsedFilters;
    }

    public static @NonNull String toSPARQL(@NonNull Expr expr) {
        return toSPARQL(new StringBuilder(128), expr, true).toString();
    }

    public static @NonNull StringBuilder
    toSPARQL(@NonNull StringBuilder out, @NonNull Expr expr, boolean root) {
        if      (expr.isVariable()              ) return out.append(expr);
        else if (expr instanceof NodeValue      ) return out.append(expr);
        else if (expr instanceof ExprNone       ) return out;
        else if (expr instanceof ExprFunction fn) return toSPARQLFunction(out, fn, root);

        throw new UnsupportedOperationException("Unsupported Expr subclass: "+expr.getClass());
    }

    private static @NonNull StringBuilder
    toSPARQLFunction(@NonNull StringBuilder out, @NonNull ExprFunction fn, boolean root) {
        String name = getFunctionName(fn);
        int nArgs = fn.numArgs();

        if (nArgs == 2 && BI_OPS.contains(name)) {
            out.append(root ? "" : "(");
            toSPARQL(out, fn.getArg(1), false).append(' ').append(name).append(' ');
            return toSPARQL(out, fn.getArg(2), false).append(root ? "" : ")");
        } else if (nArgs == 1  && UN_OPS.contains(name)) {
            Expr arg = fn.getArg(1);
            boolean par = arg instanceof ExprFunction f && BI_OPS.contains(getFunctionName(f));
            out.append(name).append(par ? "(" : "");
            return toSPARQL(out, arg, false).append(par ? ")" : "");
        } else {
            out.append(name).append('(');
            for (int i = 1; i <= nArgs; i++)
                toSPARQL(out, fn.getArg(i), false).append(',').append(' ');
            if (nArgs > 0)
                out.setLength(out.length()-2);
            return out.append(')');
        }
    }

    private static @NonNull String getFunctionName(@NonNull ExprFunction fn) {
        return fn.getOpName() == null ? fn.getFunctionName(SER_CTX) : fn.getOpName();
    }

}
