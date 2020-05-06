import logging
import pytest

from tests import disruption_helpers
from ocs_ci.ocs import constants
from ocs_ci.utility import utils
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers

log = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def fioscale(request):

    # Create RipSaw Operator
    fioscale = FioPodScale(
        kind=constants.DEPLOYMENTCONFIG, pod_dict_path=constants.FEDORA_DC_YAML,
        node_selector=constants.SCALE_NODE_SELECTOR
    )
    fioscale.create_scale_pods(
        scale_count=1500, pods_per_iter=2, io_runtime=120,
        start_io=True
    )

    def teardown():
        fioscale.cleanup()
    request.addfinalizer(teardown)
    return fioscale


@scale
@ignore_leftovers
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(
            *['mgr'], marks=[pytest.mark.polarion_id("OCS-766")]
        ),
        pytest.param(
            *['mon'], marks=[pytest.mark.polarion_id("OCS-764")]
        ),
        pytest.param(
            *['osd'], marks=[pytest.mark.polarion_id("OCS-765")]
        ),
        pytest.param(
            *['mds'], marks=[pytest.mark.polarion_id("OCS-613")]
        )
    ]
)
class TestScaleRespinCephPods(E2ETest):
    """
    Scale the OCS cluster to reach 1500 PVC+POD
    """

    def test_pv_scale_out_create_pvcs_and_respin_ceph_pods(
        self, fioscale, resource_to_delete,
    ):
        """
        Test case to scale PVC+POD with multi projects and reach expected PVC count
        """

        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        no_of_resource = disruption.resource_count
        for i in range(0, no_of_resource):
            disruption.delete_resource(resource_id=i)

        utils.ceph_health_check()
