import aws_cdk as core
import aws_cdk.assertions as assertions

from hy_phase2.hy_phase2_stack import HyPhase2Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in hy_phase2/hy_phase2_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = HyPhase2Stack(app, "hy-phase2")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
