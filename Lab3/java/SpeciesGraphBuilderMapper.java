//

// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
// Modified - Dino Konstantopoulos (dinok@bu.edu)
// Copyright 2010, BU MET CS 755 Cloud Computing
// Distributed under the "If it works, remolded by Dino Konstantopoulos,
// otherwise no idea who did! And by the way, you're free to do whatever
// you want to with it" dinolicense
//
package BU.MET.CS755;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/*
 * This class reads in a serialized download of wikispecies, extracts out the links, and
 * foreach link:
 *   emits (currPage, (linkedPage, 1))
 *
 *
 */
public class SpeciesGraphBuilderMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
    public void map(Text key, Text value,
                    OutputCollector output, Reporter reporter) throws IOException
    {
        String title = null;
        String inputString;
        ArrayList<String> outlinks = null;

        //Get the DOM Builder Factory
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        try {
            //Get the DOM Builder
            DocumentBuilder builder = factory.newDocumentBuilder();

            //Load and Parse the XML document
            //document contains the complete XML as a Tree.
            inputString = key.toString();
            InputSource is = new InputSource(new StringReader(key.toString()));
            Document    document = builder.parse(is);

	    // Look for taxonavigation marker.
            if ((inputString.indexOf("== Taxonavigation ==") == -1) &&
                (inputString.indexOf("==Taxonavigation==") == -1))
            {
                return;
            }

            // Get the title node
            NodeList nodeList = document.getDocumentElement().getChildNodes();
            NodeList theTitle = document.getElementsByTagName("title");

	    // Parse the species name from the title node.
            for (int i = 0; i < theTitle.getLength(); i++)
            {
                Node theNode = theTitle.item(i);
                Node nodeVal = theNode.getFirstChild();
                title = nodeVal.getNodeValue();
                title = title.replace(":", "_");
            }

	    // Get the sub-species list from <text>
            NodeList theText = document.getElementsByTagName("text");

            for (int i = 0; i < theText.getLength(); i++)
            {
                Node theNode = theText.item(i);
                Node nodeVal = theNode.getFirstChild();

                if (nodeVal != null)
                {
                    outlinks = GetOutlinks(nodeVal.getNodeValue());
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        if (title != null &&title.length() > 0)
        {
            reporter.setStatus(title);
        }
        else
        {
            return;
        }

        StringBuilder builder = new StringBuilder();

        if (outlinks != null)
        {
            for (String link : outlinks)
            {
                link = link.replace(" ", "_");
                link = link.replace("\n", "");
                builder.append(" ");
                builder.append(link);
            }
        }

        // remove any newlines
        if (builder.toString().contains("\n"))
        {
            builder.toString().replace("\n", "");
        }

        output.collect(new Text(title), new Text(builder.toString()));
    }

    public String GetTitle(String page, Reporter reporter) throws IOException
    {
        int end = page.indexOf(",");

        if (-1 == end)
        {
            return "";
        }

        return page.substring(0, end);
    }

    public ArrayList<String> GetOutlinks(String page)
    {
        int end;
        ArrayList<String> outlinks = new ArrayList<String>();

        int indexTaxonav = page.indexOf("== Taxonavigation ==");

        if (indexTaxonav == -1)
        {
            indexTaxonav = page.indexOf("==Taxonavigation==");
        }
	if (indexTaxonav == -1)
	{
     	    System.out.println("no taxonavigation for " + page);
	    return null;
	}

        String startParse = page.substring(indexTaxonav);

        startParse = startParse.replace("&nbsp;", " ");

        int startNames = startParse.indexOf("== Name");

        if (startNames == -1)
        {
            startNames = startParse.indexOf("==Name");
        }

        //todo: what to do if cannot demarcate between names and species?
        if (startNames == -1)
        {
            System.out.println("no name tag " + page);
            startNames = startParse.length();
        }

        int start = startParse.indexOf("[[");

        if (start > startNames)
        {
            System.out.println("no outlinks " + page);
            return null;
        }

        while (start > 0 && start < startNames)
        {
            start = start + 2;
            end   = startParse.indexOf("]]", start);

            if (end == -1)
            {
                break;
            }

            String toAdd = startParse.substring(start);
            toAdd = toAdd.substring(0, end - start);

            if (toAdd.contains(":") == false)
            {
                outlinks.add(toAdd);
            }
            else
            {
                break;
            }

            start = startParse.indexOf("[[", end + 1);
        }

        return outlinks;
    }
}
