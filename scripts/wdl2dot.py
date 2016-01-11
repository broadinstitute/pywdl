import wdl
import argparse

from wdl.binding import *

#This is a method in pywdl that needs a change in order to get 
#nodes above the current scope (for example if a call in a scatter block
#has an input from a task that came before the scatter block).
#Once that change has been made this method can be removed.
def new_upstream(self):
		hierarchy = scope_hierarchy(self)
		up = set()
		for scope in hierarchy:
			if isinstance(scope, Scatter):
				up.add(scope)
				up.update(scope.upstream())
		for expression in self.inputs.values():
			for node in wdl.find_asts(expression.ast, "MemberAccess"):
				lhs_expr = expr_str(node.attr('lhs'))
				parent = self.parent
				up_val = None
				while parent and not up_val:
					fqn = '{}.{}'.format(parent.name, lhs_expr)
					up_val = hierarchy[-1].resolve(fqn)
					parent = parent.parent
				if up_val:
					up.add(up_val)
		return up

def run():
	wdl.binding.Call.upstream = new_upstream
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input_file', help='Input wdl file', required=True, dest='input_file')
	parser.add_argument('-o', '--output_file', help='Output dot file', required=True, dest='output_file')
	args = parser.parse_args()

	fp = open(args.input_file)

	wdl_namespace = wdl.load(fp)

	output = open(args.output_file, 'w')
	output.write('digraph {\n')

	links = []
	scatter_nodes = {}

	def visit_node(node):
		if isinstance(node.parent, Scatter):
			try:
				scatter_set = scatter_nodes[node.parent.name]
			except KeyError:
				scatter_nodes[node.parent.name] = set()
				scatter_set = scatter_nodes[node.parent.name]
			scatter_set.add(node.name)

	for workflow in wdl_namespace.workflows:
		for call in workflow.calls():
			for downstream in call.downstream():
				if not isinstance(downstream, Scatter):
					links.append((call.name, downstream.name))
					visit_node(call)
					visit_node(downstream)

	for name, nodes in scatter_nodes.items():
		output.write('subgraph cluster_%s {\n' % name)
		for node in nodes:
			output.write('\t"%s"\n' % node)
		output.write('}\n')

	for from_node, to_node in links:
		output.write('"%s"->"%s"\n' % (from_node, to_node))
	output.write('}')

	output.close()
	fp.close()

if __name__ == '__main__':
    run()